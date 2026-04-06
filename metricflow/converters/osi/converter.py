from __future__ import annotations

import re
from collections import defaultdict
from itertools import combinations
from typing import Dict, List, Optional, Sequence, Set, Tuple, Union

import jinja2

from metricflow.converters.osi.models import (
    OSIDataset,
    OSIDialect,
    OSIDialectExpression,
    OSIDimension,
    OSIDocument,
    OSIExpression,
    OSIField,
    OSIMetric,
    OSIRelationship,
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
from metricflow_semantic_interfaces.protocols.dimension import Dimension
from metricflow_semantic_interfaces.protocols.entity import Entity
from metricflow_semantic_interfaces.protocols.measure import (
    Measure,
    MeasureAggregationParameters,
)
from metricflow_semantic_interfaces.protocols.metric import Metric
from metricflow_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from metricflow_semantic_interfaces.protocols.semantic_model import SemanticModel
from metricflow_semantic_interfaces.protocols.where_filter import WhereFilterIntersection
from metricflow_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    MetricType,
    TimeGranularity,
)


class MSIToOSIConverter:
    """Converts an MSI SemanticManifest into an OSI Document."""

    def __init__(self, dialect: OSIDialect = OSIDialect.ANSI_SQL) -> None:  # noqa: D107
        self._dialect = dialect

    def convert(self, manifest: SemanticManifest, model_name: str = "semantic_model") -> OSIDocument:  # noqa: D102
        datasets = [self._convert_semantic_model(sm) for sm in manifest.semantic_models]

        entity_index = self._build_entity_index(manifest.semantic_models)
        relationships = self._build_relationships(entity_index)

        measure_index = self._build_measure_index(manifest.semantic_models)
        metric_index = self._build_metric_index(manifest.metrics)
        expression_cache: Dict[Tuple[str, Optional[str]], str] = {}

        osi_metrics: List[OSIMetric] = []
        for metric in manifest.metrics:
            if metric.type is MetricType.CONVERSION:
                continue
            expr = self._resolve_metric_expression(metric, metric_index, measure_index, expression_cache)
            osi_metrics.append(
                OSIMetric(
                    name=metric.name,
                    expression=self._make_expression(expr),
                    description=metric.description,
                )
            )

        return OSIDocument(
            version="0.1.1",
            dialects=[self._dialect],
            semantic_model=[
                OSISemanticModel(
                    name=model_name,
                    datasets=datasets,
                    relationships=relationships if relationships else None,
                    metrics=osi_metrics if osi_metrics else None,
                )
            ],
        )

    def _convert_semantic_model(self, sm: SemanticModel) -> OSIDataset:
        fields: List[OSIField] = []
        for entity in sm.entities:
            fields.append(self._convert_entity(entity))
        for dim in sm.dimensions:
            fields.append(self._convert_dimension(dim))
        for measure in sm.measures:
            fields.append(self._convert_measure(measure))

        primary_key, unique_keys = self._extract_keys(sm.entities)

        return OSIDataset(
            name=sm.name,
            source=sm.node_relation.relation_name,
            primary_key=primary_key,
            unique_keys=unique_keys if unique_keys else None,
            description=sm.description,
            fields=fields if fields else None,
        )

    def _convert_dimension(self, dim: Dimension) -> OSIField:
        expr = dim.expr if dim.expr is not None else dim.name
        is_time = dim.type == DimensionType.TIME

        return OSIField(
            name=dim.name,
            expression=self._make_expression(expr),
            dimension=OSIDimension(is_time=is_time),
            label=dim.label,
            description=dim.description,
        )

    def _convert_entity(self, entity: Entity) -> OSIField:
        expr = entity.expr if entity.expr is not None else entity.name

        return OSIField(
            name=entity.name,
            expression=self._make_expression(expr),
            label=entity.label,
            description=entity.description,
        )

    def _convert_measure(self, measure: Measure) -> OSIField:
        expr = measure.expr if measure.expr is not None else measure.name

        return OSIField(
            name=measure.name,
            expression=self._make_expression(expr),
            label=measure.label,
            description=measure.description,
        )

    @staticmethod
    def _extract_keys(entities: Sequence[Entity]) -> Tuple[Optional[List[str]], List[List[str]]]:
        primary_key: Optional[List[str]] = None
        unique_keys: List[List[str]] = []

        for entity in entities:
            col = entity.expr if entity.expr is not None else entity.name
            if entity.type == EntityType.PRIMARY:
                primary_key = [col]
            elif entity.type == EntityType.UNIQUE:
                unique_keys.append([col])

        return primary_key, unique_keys

    @staticmethod
    def _build_measure_index(
        semantic_models: Sequence[SemanticModel],
    ) -> Dict[str, Tuple[AggregationType, str, Optional[MeasureAggregationParameters]]]:
        """Map measure name to (agg, col_expr, agg_params) for expression resolution."""
        index: Dict[str, Tuple[AggregationType, str, Optional[MeasureAggregationParameters]]] = {}
        for sm in semantic_models:
            for measure in sm.measures:
                col = measure.expr if measure.expr is not None else measure.name
                index[measure.name] = (measure.agg, col, measure.agg_params)
        return index

    @staticmethod
    def _build_metric_index(metrics: Sequence[Metric]) -> Dict[str, Metric]:
        """Map metric name to Metric for recursive resolution."""
        return {metric.name: metric for metric in metrics}

    @staticmethod
    def _lookup_metric(metric_index: Dict[str, Metric], name: str, context: str) -> Metric:
        """Look up a metric by name, raising a clear ValueError if not found."""
        try:
            return metric_index[name]
        except KeyError:
            raise ValueError(f"{context}: references unknown metric '{name}'")

    def _resolve_metric_expression(
        self,
        metric: Metric,
        metric_index: Dict[str, Metric],
        measure_index: Dict[str, Tuple[AggregationType, str, Optional[MeasureAggregationParameters]]],
        cache: Dict[Tuple[str, Optional[str]], str],
        parent_filter: Optional[str] = None,
    ) -> str:
        """Recursively resolve a metric to a fully-inlined SQL expression string."""
        own_filter = _collect_filter_sql(metric.filter)
        combined_filter = _merge_filter_sqls(parent_filter, own_filter)

        cache_key = (metric.name, combined_filter)
        if cache_key in cache:
            return cache[cache_key]

        if metric.type is MetricType.SIMPLE:
            expr = self._resolve_simple(metric, measure_index, combined_filter)
        elif metric.type is MetricType.CUMULATIVE:
            expr = self._resolve_cumulative(metric, metric_index, measure_index, cache, combined_filter)
        elif metric.type is MetricType.RATIO:
            expr = self._resolve_ratio(metric, metric_index, measure_index, cache, combined_filter)
        elif metric.type is MetricType.DERIVED:
            expr = self._resolve_derived(metric, metric_index, measure_index, cache, combined_filter)
        else:
            expr = metric.name

        cache[cache_key] = expr
        return expr

    def _resolve_simple(
        self,
        metric: Metric,
        measure_index: Dict[str, Tuple[AggregationType, str, Optional[MeasureAggregationParameters]]],
        filter_sql: Optional[str] = None,
    ) -> str:
        """Resolve a SIMPLE metric: look up its measure or use metric_aggregation_params."""
        if metric.type_params.measure is not None:
            measure_name = metric.type_params.measure.name
            agg, col, agg_params = measure_index[measure_name]
            measure_filter = _collect_filter_sql(metric.type_params.measure.filter)
            return self._build_agg_expression(agg, col, agg_params, _merge_filter_sqls(filter_sql, measure_filter))

        # metric_aggregation_params path: aggregation info lives on the metric itself
        agg_params_obj = metric.type_params.metric_aggregation_params
        if agg_params_obj is None:
            raise ValueError(f"SIMPLE metric '{metric.name}' has neither measure nor metric_aggregation_params")
        col = metric.type_params.expr if metric.type_params.expr is not None else metric.name
        return self._build_agg_expression(agg_params_obj.agg, col, agg_params_obj.agg_params, filter_sql)

    def _resolve_cumulative(
        self,
        metric: Metric,
        metric_index: Dict[str, Metric],
        measure_index: Dict[str, Tuple[AggregationType, str, Optional[MeasureAggregationParameters]]],
        cache: Dict[Tuple[str, Optional[str]], str],
        filter_sql: Optional[str] = None,
    ) -> str:
        """Resolve a CUMULATIVE metric to its base aggregation expression.

        Window/grain semantics are not representable in an OSI expression string.
        """
        if metric.type_params.measure is not None:
            measure_name = metric.type_params.measure.name
            agg, col, agg_params = measure_index[measure_name]
            measure_filter = _collect_filter_sql(metric.type_params.measure.filter)
            return self._build_agg_expression(agg, col, agg_params, _merge_filter_sqls(filter_sql, measure_filter))

        # cumulative_type_params.metric path: recurse into the referenced metric
        cumulative_params = metric.type_params.cumulative_type_params
        if cumulative_params is None or cumulative_params.metric is None:
            raise ValueError(f"CUMULATIVE metric '{metric.name}' has no resolvable measure or sub-metric")
        sub_input = cumulative_params.metric
        sub_filter = _merge_filter_sqls(filter_sql, _collect_filter_sql(sub_input.filter))
        return self._resolve_metric_expression(
            self._lookup_metric(metric_index, sub_input.name, f"CUMULATIVE metric '{metric.name}'"),
            metric_index,
            measure_index,
            cache,
            sub_filter,
        )

    def _resolve_ratio(
        self,
        metric: Metric,
        metric_index: Dict[str, Metric],
        measure_index: Dict[str, Tuple[AggregationType, str, Optional[MeasureAggregationParameters]]],
        cache: Dict[Tuple[str, Optional[str]], str],
        filter_sql: Optional[str] = None,
    ) -> str:
        """Resolve a RATIO metric as (numerator) / (denominator), both fully inlined."""
        if metric.type_params.numerator is None or metric.type_params.denominator is None:
            raise ValueError(f"RATIO metric '{metric.name}' is missing numerator or denominator")
        num_input = metric.type_params.numerator
        den_input = metric.type_params.denominator
        num_filter = _merge_filter_sqls(filter_sql, _collect_filter_sql(num_input.filter))
        den_filter = _merge_filter_sqls(filter_sql, _collect_filter_sql(den_input.filter))
        num_expr = self._resolve_metric_expression(
            self._lookup_metric(metric_index, num_input.name, f"RATIO metric '{metric.name}' numerator"),
            metric_index,
            measure_index,
            cache,
            num_filter,
        )
        den_expr = self._resolve_metric_expression(
            self._lookup_metric(metric_index, den_input.name, f"RATIO metric '{metric.name}' denominator"),
            metric_index,
            measure_index,
            cache,
            den_filter,
        )
        return f"({num_expr}) / ({den_expr})"

    def _resolve_derived(
        self,
        metric: Metric,
        metric_index: Dict[str, Metric],
        measure_index: Dict[str, Tuple[AggregationType, str, Optional[MeasureAggregationParameters]]],
        cache: Dict[Tuple[str, Optional[str]], str],
        filter_sql: Optional[str] = None,
    ) -> str:
        """Resolve a DERIVED metric by substituting each input metric's expression into the expr string.

        Compound sub-expressions (DERIVED/RATIO) are wrapped in parentheses to preserve operator precedence.
        """
        expr = metric.type_params.expr or ""
        for input_metric in metric.type_params.metrics or []:
            ref = input_metric.alias if input_metric.alias else input_metric.name
            dep_metric = self._lookup_metric(metric_index, input_metric.name, f"DERIVED metric '{metric.name}'")
            input_filter = _merge_filter_sqls(filter_sql, _collect_filter_sql(input_metric.filter))
            resolved = self._resolve_metric_expression(dep_metric, metric_index, measure_index, cache, input_filter)
            if dep_metric.type in (MetricType.DERIVED, MetricType.RATIO):
                resolved = f"({resolved})"
            expr = re.sub(rf"\b{re.escape(ref)}\b", resolved, expr)
        return expr

    @staticmethod
    def _build_entity_index(
        semantic_models: Sequence[SemanticModel],
    ) -> Dict[str, List[Tuple[str, str, EntityType]]]:
        """Map each entity name to the (dataset_name, column, entity_type) tuples that declare it."""
        index: Dict[str, List[Tuple[str, str, EntityType]]] = defaultdict(list)
        for sm in semantic_models:
            for entity in sm.entities:
                if entity.type == EntityType.NATURAL:
                    continue
                col = entity.expr if entity.expr is not None else entity.name
                index[entity.name].append((sm.name, col, entity.type))
        return dict(index)

    @staticmethod
    def _relationship_direction(
        ds_a: str, col_a: str, type_a: EntityType, ds_b: str, col_b: str, type_b: EntityType
    ) -> Tuple[str, str, str, str]:
        """Return (from_ds, to_ds, from_col, to_col) obeying OSI directionality.

        OSI spec: ``from`` is the many-side (FK holder), ``to`` is the one-side (PK holder).
        FOREIGN entities are always the many-side; PRIMARY/UNIQUE are the one-side.
        When both sides share the same cardinality tier, break ties alphabetically by dataset name.
        """
        one_side_types = {EntityType.PRIMARY, EntityType.UNIQUE}
        a_is_one_side = type_a in one_side_types
        b_is_one_side = type_b in one_side_types

        if a_is_one_side and not b_is_one_side:
            return ds_b, ds_a, col_b, col_a
        if b_is_one_side and not a_is_one_side:
            return ds_a, ds_b, col_a, col_b
        # Same cardinality tier — use alphabetical order for determinism.
        if ds_a <= ds_b:
            return ds_a, ds_b, col_a, col_b
        return ds_b, ds_a, col_b, col_a

    @staticmethod
    def _build_relationships(
        entity_index: Dict[str, List[Tuple[str, str, EntityType]]],
    ) -> List[OSIRelationship]:
        """Resolve implicit MSI entity links into explicit OSI relationships.

        Every pair of datasets sharing an entity name is a valid join path.
        """
        relationships: List[OSIRelationship] = []
        for entity_name, entries in entity_index.items():
            for (ds_a, col_a, type_a), (ds_b, col_b, type_b) in combinations(entries, 2):
                if ds_a == ds_b:
                    continue
                from_ds, to_ds, from_col, to_col = MSIToOSIConverter._relationship_direction(
                    ds_a, col_a, type_a, ds_b, col_b, type_b
                )
                relationships.append(
                    OSIRelationship(
                        name=f"{from_ds}__{to_ds}__{entity_name}",
                        from_dataset=from_ds,
                        to=to_ds,
                        from_columns=[from_col],
                        to_columns=[to_col],
                    )
                )
        return relationships

    @staticmethod
    def _build_agg_expression(
        agg: AggregationType,
        col: str,
        agg_params: Optional[MeasureAggregationParameters],
        filter_sql: Optional[str] = None,
    ) -> str:
        # Inject the filter as CASE WHEN inside the aggregation.  NULL values
        # produced by CASE WHEN are ignored by all standard SQL aggregate
        # functions, preserving correct filtering semantics.
        fc = f"CASE WHEN {filter_sql} THEN {col} END" if filter_sql else col

        if agg == AggregationType.SUM:
            return f"SUM({fc})"
        if agg == AggregationType.MIN:
            return f"MIN({fc})"
        if agg == AggregationType.MAX:
            return f"MAX({fc})"
        if agg == AggregationType.COUNT:
            return f"COUNT({fc})"
        if agg == AggregationType.COUNT_DISTINCT:
            return f"COUNT(DISTINCT {fc})"
        if agg == AggregationType.AVERAGE:
            return f"AVG({fc})"
        if agg == AggregationType.SUM_BOOLEAN:
            # col is already a boolean condition; the filter becomes an extra AND term.
            if filter_sql:
                return f"SUM(CASE WHEN ({filter_sql}) AND ({col}) THEN 1 ELSE 0 END)"
            return f"SUM(CASE WHEN {col} THEN 1 ELSE 0 END)"
        if agg == AggregationType.MEDIAN:
            return f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {fc})"
        if agg == AggregationType.PERCENTILE:
            percentile = agg_params.percentile if agg_params and agg_params.percentile is not None else 0.5
            use_discrete = agg_params.use_discrete_percentile if agg_params else False
            func = "PERCENTILE_DISC" if use_discrete else "PERCENTILE_CONT"
            return f"{func}({percentile}) WITHIN GROUP (ORDER BY {fc})"
        return f"{agg.value.upper()}({fc})"

    def _make_expression(self, expr: str) -> OSIExpression:
        return OSIExpression(dialects=[OSIDialectExpression(dialect=self._dialect, expression=expr)])


# ---------------------------------------------------------------------------
# Where-filter rendering helpers
# ---------------------------------------------------------------------------


class _DimensionStub:
    """Jinja sandbox stub for ``{{ Dimension('entity__dim') }}``.

    Renders to the qualified column name, e.g. ``order__status``.
    Method chaining (``grain``, ``date_part``) appends a ``__<suffix>`` part.
    """

    def __init__(self, name: str, entity_path: Sequence[str] = ()) -> None:  # noqa: D107
        self._col = "__".join(list(entity_path) + [name])
        self._suffix = ""

    def grain(self, time_granularity: str) -> "_DimensionStub":  # noqa: D102
        self._suffix = f"__{time_granularity.lower()}"
        return self

    def date_part(self, date_part_name: str) -> "_DimensionStub":  # noqa: D102
        self._suffix = f"__{date_part_name.lower()}"
        return self

    def descending(self, _is_descending: bool) -> "_DimensionStub":  # noqa: D102
        return self

    def __str__(self) -> str:
        return f"{self._col}{self._suffix}"


class _TimeDimensionStub:
    """Jinja sandbox stub for ``{{ TimeDimension('entity__dim', 'grain') }}``.

    Renders to ``entity__dim`` or ``entity__dim__grain`` when a granularity is provided.
    """

    def __init__(
        self,
        name: str,
        time_granularity_name: Optional[str] = None,
        entity_path: Sequence[str] = (),
        **_kwargs: object,
    ) -> None:  # noqa: D107
        self._col = "__".join(list(entity_path) + [name])
        self._grain = time_granularity_name

    def grain(self, time_granularity: str) -> "_TimeDimensionStub":  # noqa: D102
        self._grain = time_granularity
        return self

    def date_part(self, date_part_name: str) -> "_TimeDimensionStub":  # noqa: D102
        self._grain = date_part_name
        return self

    def descending(self, _is_descending: bool) -> "_TimeDimensionStub":  # noqa: D102
        return self

    def __str__(self) -> str:
        if self._grain:
            return f"{self._col}__{self._grain.lower()}"
        return self._col


class _EntityStub:
    """Jinja sandbox stub for ``{{ Entity('name') }}``."""

    def __init__(self, name: str, entity_path: Sequence[str] = ()) -> None:  # noqa: D107
        self._col = "__".join(list(entity_path) + [name])

    def descending(self, _is_descending: bool) -> "_EntityStub":  # noqa: D102
        return self

    def __str__(self) -> str:
        return self._col


class _MetricStub:
    """Jinja sandbox stub for ``{{ Metric('name') }}``."""

    def __init__(self, name: str, group_by: Sequence[str] = ()) -> None:  # noqa: D107
        self._name = name

    def descending(self, _is_descending: bool) -> "_MetricStub":  # noqa: D102
        return self

    def __str__(self) -> str:
        return self._name


def _render_filter_template(template: str) -> str:
    """Render an MSI where-filter Jinja template to a plain SQL fragment.

    Jinja references such as ``{{ Dimension('order__status') }}``,
    ``{{ TimeDimension('order__ds', 'day') }}``, ``{{ Entity('user') }}``,
    and ``{{ Metric('revenue') }}`` are resolved to their column-name
    equivalents using lightweight stubs.  The output is a best-effort SQL
    string suitable for embedding in an OSI expression.
    """
    return jinja2.Template(template, undefined=jinja2.StrictUndefined).render(
        Dimension=_DimensionStub,
        TimeDimension=_TimeDimensionStub,
        Entity=_EntityStub,
        Metric=_MetricStub,
    )


def _collect_filter_sql(*filters: Optional[WhereFilterIntersection]) -> Optional[str]:
    """Render and merge MSI WhereFilterIntersection objects into a single SQL fragment.

    Jinja references (e.g. ``{{ Dimension('order__status') }}``) are resolved
    using lightweight stubs that produce MetricFlow-qualified column names such
    as ``order__status``.  These are *not* fully resolved SQL column aliases —
    resolving to actual table column names would require ``WhereFilterSpecFactory``
    and ``ColumnAssociationResolver`` from ``metricflow_semantics``, which is out
    of scope here.  OSI consumers are expected to perform their own column
    resolution against the source data.
    """
    parts: List[str] = []
    for f in filters:
        if f is None:
            continue
        for wf in f.where_filters:
            rendered = _render_filter_template(wf.where_sql_template).strip()
            if rendered:
                parts.append(rendered)
    return _merge_filter_sqls(*parts)


def _merge_filter_sqls(*parts: Optional[str]) -> Optional[str]:
    """Join non-None SQL filter strings with AND, wrapping each in parens when multiple."""
    active = [p for p in parts if p]
    if not active:
        return None
    if len(active) == 1:
        return active[0]
    return " AND ".join(f"({p})" for p in active)


# ---------------------------------------------------------------------------
# Reverse converter: OSI → MSI
# ---------------------------------------------------------------------------

# Keyed by (dataset_name_or_None, col_name) → (AggregationType, bare_col_expr).
# None as dataset name means the column reference was unqualified in the expression.
_MeasureKey = Tuple[Optional[str], str]

_SIMPLE_AGG_MAP: Dict[str, AggregationType] = {
    "SUM": AggregationType.SUM,
    "COUNT": AggregationType.COUNT,
    "AVG": AggregationType.AVERAGE,
    "MIN": AggregationType.MIN,
    "MAX": AggregationType.MAX,
}


def _strip_qualifier(col: str) -> str:
    """Strip a leading dataset qualifier, e.g. 'orders.amount' → 'amount'."""
    return col.rsplit(".", 1)[-1] if "." in col else col


def _extract_agg_info(expression: str) -> Optional[Tuple[AggregationType, str]]:
    """Parse a simple SQL aggregation expression.

    Returns ``(agg_type, bare_col)`` for recognised patterns, ``None`` otherwise.
    The returned column name has any dataset qualifier stripped.
    """
    expr = expression.strip()

    # COUNT(DISTINCT col)
    m = re.fullmatch(r"COUNT\s*\(\s*DISTINCT\s+(.+?)\s*\)", expr, re.IGNORECASE)
    if m:
        return AggregationType.COUNT_DISTINCT, _strip_qualifier(m.group(1))

    # SUM(CASE WHEN col THEN 1 ELSE 0 END)
    m = re.fullmatch(r"SUM\s*\(\s*CASE\s+WHEN\s+(.+?)\s+THEN\s+1\s+ELSE\s+0\s+END\s*\)", expr, re.IGNORECASE)
    if m:
        return AggregationType.SUM_BOOLEAN, _strip_qualifier(m.group(1))

    # PERCENTILE_CONT/DISC(p) WITHIN GROUP (ORDER BY col)
    m = re.fullmatch(
        r"(PERCENTILE_CONT|PERCENTILE_DISC)\s*\(\s*([0-9.]+)\s*\)\s*WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+(.+?)\s*\)",
        expr,
        re.IGNORECASE,
    )
    if m:
        p = float(m.group(2))
        col = _strip_qualifier(m.group(3))
        if p == 0.5 and m.group(1).upper() == "PERCENTILE_CONT":
            return AggregationType.MEDIAN, col
        return AggregationType.PERCENTILE, col

    # Simple: SUM(col), COUNT(col), AVG(col), MIN(col), MAX(col)
    # Use [^()]+ to reject expressions with nested parentheses (e.g. SUM(a) + SUM(b)).
    m = re.fullmatch(r"([A-Za-z_]+)\s*\(\s*([^()]+?)\s*\)", expr)
    if m:
        func = m.group(1).upper()
        col = _strip_qualifier(m.group(2))
        if func in _SIMPLE_AGG_MAP:
            return _SIMPLE_AGG_MAP[func], col

    return None


def _try_parse_ratio(expr_str: str) -> Optional[Tuple[str, str]]:
    """Try to parse ``(expr_a) / (expr_b)`` returning ``(num_expr, den_expr)`` or None."""
    s = expr_str.strip()
    if not s.startswith("("):
        return None
    depth = 0
    for i, ch in enumerate(s):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                rest = s[i + 1 :].strip()
                if rest.startswith("/"):
                    num_expr = s[1:i].strip()
                    den_part = rest[1:].strip()
                    if den_part.startswith("(") and den_part.endswith(")"):
                        den_expr = den_part[1:-1].strip()
                    else:
                        den_expr = den_part
                    return num_expr, den_expr
                break
    return None


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


def _get_raw_inner_col(expression: str) -> Optional[str]:
    """Extract the raw column reference from inside a simple aggregation, before stripping qualifiers."""
    expr = expression.strip()
    m = re.fullmatch(r"[A-Za-z_]+\s*\(\s*(.+?)\s*\)", expr)
    if m:
        return m.group(1).strip()
    return None
