from __future__ import annotations

import re
from collections import defaultdict
from itertools import combinations
from typing import Dict, List, Optional, Sequence, Tuple

from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from metricflow.converters.filter_utils import _collect_filter_sql, _merge_filter_sqls
from metricflow.converters.models import (
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
from metricflow_semantic_interfaces.enum_extension import assert_values_exhausted
from metricflow_semantic_interfaces.protocols.dimension import Dimension
from metricflow_semantic_interfaces.protocols.entity import Entity
from metricflow_semantic_interfaces.protocols.measure import (
    Measure,
    MeasureAggregationParameters,
)
from metricflow_semantic_interfaces.protocols.metric import Metric
from metricflow_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from metricflow_semantic_interfaces.protocols.semantic_model import SemanticModel
from metricflow_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    MetricType,
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
            raise ValueError(LazyFormat("references unknown metric", context=context, metric_name=name))

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
        elif metric.type is MetricType.CONVERSION:
            # CONVERSION metrics are skipped in convert(); this branch should never be reached.
            raise RuntimeError(
                LazyFormat("Unexpected CONVERSION metric in expression resolver", metric_name=metric.name)
            )
        else:
            assert_values_exhausted(metric.type)

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
            raise ValueError(
                LazyFormat(
                    "SIMPLE metric has neither measure nor metric_aggregation_params",
                    metric_name=metric.name,
                )
            )
        col = metric.type_params.expr if metric.type_params.expr is not None else metric.name
        # Qualify the column with the semantic model name so the OSI expression is
        # unambiguous and OSI→MSI conversion can recover the dataset from the qualifier.
        if "." not in col:
            col = f"{agg_params_obj.semantic_model}.{col}"
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
            raise ValueError(
                LazyFormat(
                    "CUMULATIVE metric has no resolvable measure or sub-metric",
                    metric_name=metric.name,
                )
            )
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
            raise ValueError(
                LazyFormat(
                    "RATIO metric is missing numerator or denominator",
                    metric_name=metric.name,
                )
            )
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
        elif agg == AggregationType.MIN:
            return f"MIN({fc})"
        elif agg == AggregationType.MAX:
            return f"MAX({fc})"
        elif agg == AggregationType.COUNT:
            return f"COUNT({fc})"
        elif agg == AggregationType.COUNT_DISTINCT:
            return f"COUNT(DISTINCT {fc})"
        elif agg == AggregationType.AVERAGE:
            return f"AVG({fc})"
        elif agg == AggregationType.SUM_BOOLEAN:
            # col is already a boolean condition; the filter becomes an extra AND term.
            if filter_sql:
                return f"SUM(CASE WHEN ({filter_sql}) AND ({col}) THEN 1 ELSE 0 END)"
            return f"SUM(CASE WHEN {col} THEN 1 ELSE 0 END)"
        elif agg == AggregationType.MEDIAN:
            return f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {fc})"
        elif agg == AggregationType.PERCENTILE:
            percentile = agg_params.percentile if agg_params and agg_params.percentile is not None else 0.5
            use_discrete = agg_params.use_discrete_percentile if agg_params else False
            func = "PERCENTILE_DISC" if use_discrete else "PERCENTILE_CONT"
            return f"{func}({percentile}) WITHIN GROUP (ORDER BY {fc})"
        else:
            assert_values_exhausted(agg)

    def _make_expression(self, expr: str) -> OSIExpression:
        return OSIExpression(dialects=[OSIDialectExpression(dialect=self._dialect, expression=expr)])
