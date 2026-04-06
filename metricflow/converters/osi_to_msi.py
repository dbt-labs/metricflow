from __future__ import annotations

from typing import Dict, List, Optional, Set, Tuple, Union

from metricflow.converters.expression_utils import (
    _extract_agg_info,
    _get_raw_inner_col,
    _MeasureKey,
    _strip_qualifier,
    _try_parse_ratio,
)
from metricflow.converters.models import (
    OSIDataset,
    OSIDialect,
    OSIDocument,
    OSIExpression,
    OSIField,
    OSISemanticModel,
)
from metricflow_semantic_interfaces.implementations.elements.dimension import (
    PydanticDimension,
    PydanticDimensionTypeParams,
)
from metricflow_semantic_interfaces.implementations.elements.entity import PydanticEntity
from metricflow_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from metricflow_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricInput,
    PydanticMetricInputMeasure,
    PydanticMetricTypeParams,
)
from metricflow_semantic_interfaces.implementations.project_configuration import (
    PydanticProjectConfiguration,
)
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.implementations.semantic_model import (
    PydanticNodeRelation,
    PydanticSemanticModel,
)
from metricflow_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    MetricType,
    TimeGranularity,
)


class OSIToMSIConverter:
    """Converts an OSI Document into a PydanticSemanticManifest.

    The conversion is inherently lossy: OSI stores metrics as raw SQL expressions
    and carries no metric-type metadata (SIMPLE / RATIO / CUMULATIVE / …).  The
    converter reconstructs a best-effort MSI manifest using the following rules:

    * Datasets → one PydanticSemanticModel each.
    * Fields are classified as entities, dimensions, or measures using key and
      relationship metadata plus metric-expression scanning (see below).
    * Time dimensions always receive ``TimeGranularity.DAY`` — OSI has no
      granularity field.
    * Metric expressions are parsed with regex:
      - single-agg patterns (``SUM(col)``, ``COUNT(DISTINCT col)``, …) → SIMPLE
      - ``(expr_a) / (expr_b)`` → RATIO (with auto-generated sub-metrics)
      - anything else → SIMPLE pointing to a synthetic measure
    """

    def __init__(self, dialect: OSIDialect = OSIDialect.ANSI_SQL) -> None:  # noqa: D107
        self._dialect = dialect

    def convert(self, document: OSIDocument) -> PydanticSemanticManifest:  # noqa: D102
        semantic_models: List[PydanticSemanticModel] = []
        metrics: List[PydanticMetric] = []

        for osi_sm in document.semantic_model:
            measure_index = self._build_measure_index(osi_sm)
            for dataset in osi_sm.datasets:
                semantic_models.append(self._convert_dataset(dataset, osi_sm, measure_index))
            metrics.extend(self._convert_metrics(osi_sm))

        return PydanticSemanticManifest(
            semantic_models=semantic_models,
            metrics=metrics,
            project_configuration=PydanticProjectConfiguration(),
        )

    # ------------------------------------------------------------------
    # Dataset conversion
    # ------------------------------------------------------------------

    def _convert_dataset(
        self,
        dataset: OSIDataset,
        osi_sm: OSISemanticModel,
        measure_index: Dict[_MeasureKey, Tuple[AggregationType, str]],
    ) -> PydanticSemanticModel:
        primary_key_cols, unique_key_cols, foreign_key_cols = self._build_key_sets(dataset, osi_sm)

        entities: List[PydanticEntity] = []
        dimensions: List[PydanticDimension] = []
        measures: List[PydanticMeasure] = []

        for field in dataset.fields or []:
            expr = self._get_expression(field.expression)
            expr_or_none = expr if expr != field.name else None
            element = self._classify_field(
                field,
                expr,
                expr_or_none,
                dataset.name,
                primary_key_cols,
                unique_key_cols,
                foreign_key_cols,
                measure_index,
            )
            if isinstance(element, PydanticEntity):
                entities.append(element)
            elif isinstance(element, PydanticMeasure):
                measures.append(element)
            else:
                dimensions.append(element)

        return PydanticSemanticModel(
            name=dataset.name,
            node_relation=self._parse_source(dataset.source),
            description=dataset.description,
            entities=entities,
            dimensions=dimensions,
            measures=measures,
        )

    @staticmethod
    def _build_key_sets(dataset: OSIDataset, osi_sm: OSISemanticModel) -> Tuple[Set[str], Set[str], Set[str]]:
        """Return (primary_key_cols, unique_key_cols, foreign_key_cols) for a dataset."""
        primary_key_cols: Set[str] = set(dataset.primary_key or [])
        unique_key_cols: Set[str] = {col for keys in (dataset.unique_keys or []) for col in keys}
        foreign_key_cols: Set[str] = {
            col for rel in (osi_sm.relationships or []) if rel.from_dataset == dataset.name for col in rel.from_columns
        }
        return primary_key_cols, unique_key_cols, foreign_key_cols

    def _classify_field(
        self,
        field: OSIField,
        expr: str,
        expr_or_none: Optional[str],
        dataset_name: str,
        primary_key_cols: Set[str],
        unique_key_cols: Set[str],
        foreign_key_cols: Set[str],
        measure_index: Dict[_MeasureKey, Tuple[AggregationType, str]],
    ) -> Union[PydanticEntity, PydanticDimension, PydanticMeasure]:
        """Classify a single OSI field into an MSI entity, dimension, or measure.

        Classification order (first match wins):
        1. primary_key → PRIMARY entity
        2. unique_keys → UNIQUE entity
        3. foreign key (from relationship) → FOREIGN entity
        4. dimension.is_time → TIME dimension (granularity defaults to DAY)
        5. referenced in a metric aggregation expression → measure
        6. fallback → CATEGORICAL dimension
        """
        if field.name in primary_key_cols:
            return PydanticEntity(
                name=field.name,
                type=EntityType.PRIMARY,
                expr=expr_or_none,
                description=field.description,
                label=field.label,
                role=None,
                config=None,
            )
        if field.name in unique_key_cols:
            return PydanticEntity(
                name=field.name,
                type=EntityType.UNIQUE,
                expr=expr_or_none,
                description=field.description,
                label=field.label,
                role=None,
                config=None,
            )
        if field.name in foreign_key_cols:
            return PydanticEntity(
                name=field.name,
                type=EntityType.FOREIGN,
                expr=expr_or_none,
                description=field.description,
                label=field.label,
                role=None,
                config=None,
            )
        if field.dimension is not None and field.dimension.is_time:
            # OSI carries no granularity metadata; default to DAY.
            return PydanticDimension(
                name=field.name,
                type=DimensionType.TIME,
                type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.DAY),
                expr=expr_or_none,
                description=field.description,
                label=field.label,
                config=None,
            )
        if self._in_measure_index(field.name, expr, dataset_name, measure_index):
            agg, _ = (
                measure_index.get((dataset_name, field.name))
                or measure_index.get((None, field.name))
                or measure_index.get((dataset_name, _strip_qualifier(expr)))
                or measure_index[(None, _strip_qualifier(expr))]
            )
            return PydanticMeasure(
                name=field.name,
                agg=agg,
                expr=expr_or_none,
                description=field.description,
                label=field.label,
                create_metric=None,
                agg_params=None,
            )
        return PydanticDimension(
            name=field.name,
            type=DimensionType.CATEGORICAL,
            type_params=None,
            expr=expr_or_none,
            description=field.description,
            label=field.label,
            config=None,
        )

    # ------------------------------------------------------------------
    # Metric conversion
    # ------------------------------------------------------------------

    def _convert_metrics(self, osi_sm: OSISemanticModel) -> List[PydanticMetric]:
        metrics: List[PydanticMetric] = []
        for metric in osi_sm.metrics or []:
            expr_str = self._get_expression(metric.expression)
            metrics.extend(self._convert_metric(metric.name, expr_str, metric.description))
        return metrics

    def _convert_metric(self, name: str, expr_str: str, description: Optional[str]) -> List[PydanticMetric]:
        """Return one or more PydanticMetric objects for the given OSI expression.

        Multiple metrics are returned when a RATIO metric requires auto-generated
        sub-metrics for its numerator and denominator.
        """
        # --- SIMPLE: single aggregation ---
        agg_result = _extract_agg_info(expr_str)
        if agg_result is not None:
            _agg, col = agg_result
            return [
                PydanticMetric(
                    name=name,
                    description=description,
                    type=MetricType.SIMPLE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=col, filter=None, alias=None),
                    ),
                    filter=None,
                    metadata=None,
                    config=None,
                )
            ]

        # --- RATIO: (num_expr) / (den_expr) ---
        ratio_result = _try_parse_ratio(expr_str)
        if ratio_result is not None:
            num_expr, den_expr = ratio_result
            num_name = f"{name}__numerator"
            den_name = f"{name}__denominator"
            num_metrics = self._convert_metric(num_name, num_expr, None)
            den_metrics = self._convert_metric(den_name, den_expr, None)
            ratio_metric = PydanticMetric(
                name=name,
                description=description,
                type=MetricType.RATIO,
                type_params=PydanticMetricTypeParams(
                    numerator=PydanticMetricInput(name=num_name, filter=None, alias=None),
                    denominator=PydanticMetricInput(name=den_name, filter=None, alias=None),
                ),
                filter=None,
                metadata=None,
                config=None,
            )
            return [*num_metrics, *den_metrics, ratio_metric]

        # --- Fallback: create a synthetic measure reference ---
        # The expression is too complex to decompose.  We produce a SIMPLE metric
        # that points to a synthetic measure named ``{name}__expr``.  The caller is
        # responsible for ensuring a matching measure exists in the manifest.
        synthetic_measure_name = f"{name}__expr"
        return [
            PydanticMetric(
                name=name,
                description=description,
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    measure=PydanticMetricInputMeasure(name=synthetic_measure_name, filter=None, alias=None),
                ),
                filter=None,
                metadata=None,
                config=None,
            )
        ]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _in_measure_index(
        field_name: str,
        field_expr: str,
        dataset_name: str,
        measure_index: Dict[_MeasureKey, Tuple[AggregationType, str]],
    ) -> bool:
        """Return True if this field should be classified as a measure.

        Checks both the field name and the field's bare expression so that a field
        named ``revenue`` with ``expr="amount"`` is still recognised when the metric
        references ``SUM(amount)``.
        """
        bare_expr = _strip_qualifier(field_expr)
        return (
            (dataset_name, field_name) in measure_index
            or (None, field_name) in measure_index
            or (dataset_name, bare_expr) in measure_index
            or (None, bare_expr) in measure_index
        )

    def _get_expression(self, osi_expr: OSIExpression) -> str:
        """Return the expression string for the preferred dialect (fallback: first available)."""
        for dialect_expr in osi_expr.dialects:
            if dialect_expr.dialect == self._dialect:
                return dialect_expr.expression
        return osi_expr.dialects[0].expression if osi_expr.dialects else ""

    @staticmethod
    def _parse_source(source: str) -> PydanticNodeRelation:
        """Parse ``schema.table`` or ``db.schema.table`` into a PydanticNodeRelation."""
        parts = source.split(".")
        if len(parts) >= 3:
            database, schema, alias = parts[0], parts[1], ".".join(parts[2:])
            return PydanticNodeRelation(alias=alias, schema_name=schema, database=database)
        if len(parts) == 2:
            schema, alias = parts
            return PydanticNodeRelation(alias=alias, schema_name=schema)
        return PydanticNodeRelation(alias=source, schema_name="")

    @staticmethod
    def _build_measure_index(
        osi_sm: OSISemanticModel,
    ) -> Dict[_MeasureKey, Tuple[AggregationType, str]]:
        """Scan metric expressions and build a map of column references to aggregation info.

        Keys are ``(dataset_name_or_None, col_name)``.  A ``None`` dataset key means
        the column reference in the expression had no dataset qualifier.
        """
        index: Dict[_MeasureKey, Tuple[AggregationType, str]] = {}
        for metric in osi_sm.metrics or []:
            for dialect_expr in metric.expression.dialects:
                result = _extract_agg_info(dialect_expr.expression)
                if result is None:
                    continue
                agg_type, col = result
                # col might still retain a qualifier if _extract_agg_info didn't strip it;
                # _strip_qualifier is already applied inside _extract_agg_info, so col is bare.
                # Try to recover original qualified form to determine dataset.
                raw_inner = _get_raw_inner_col(dialect_expr.expression)
                if raw_inner and "." in raw_inner:
                    ds, bare = raw_inner.rsplit(".", 1)
                    index.setdefault((ds, bare), (agg_type, bare))
                index.setdefault((None, col), (agg_type, col))
        return index
