from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence, Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.protocols import Metric, WhereFilterIntersection
from dbt_semantic_interfaces.references import (
    MetricReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums import MetricType
from typing_extensions import override

from metricflow_semantics.collection_helpers.lru_cache import LruCache
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow_semantics.query.issues.issues_base import (
    MetricFlowQueryResolutionIssue,
    MetricFlowQueryResolutionIssueSet,
)
from metricflow_semantics.query.issues.parsing.cumulative_metric_requires_metric_time import (
    CumulativeMetricRequiresMetricTimeIssue,
)
from metricflow_semantics.query.issues.parsing.offset_metric_requires_metric_time import (
    OffsetMetricRequiresMetricTimeIssue,
)
from metricflow_semantics.query.issues.parsing.scd_requires_metric_time import (
    ScdRequiresMetricTimeIssue,
)
from metricflow_semantics.query.resolver_inputs.query_resolver_inputs import (
    ResolverInputForQuery,
)
from metricflow_semantics.query.validation_rules.base_validation_rule import PostResolutionQueryValidationRule
from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec


@dataclass(frozen=True)
class QueryItemsAnalysis:
    """Contains data about which items a query contains."""

    scds: Sequence[InstanceSpec]
    has_metric_time: bool
    has_agg_time_dimension: bool


class MetricTimeQueryValidationRule(PostResolutionQueryValidationRule):
    """Validates cases where a query requires metric_time to be specified as a group-by-item.

    Currently, known cases are:

    * Cumulative metrics.
    * Derived metrics with an offset time.
    * Slowly changing dimensions
    """

    def __init__(  # noqa: D107
        self, manifest_lookup: SemanticManifestLookup, resolver_input_for_query: ResolverInputForQuery
    ) -> None:
        super().__init__(manifest_lookup=manifest_lookup, resolver_input_for_query=resolver_input_for_query)

        self._metric_time_specs = tuple(
            TimeDimensionSpec.generate_possible_specs_for_time_dimension(
                time_dimension_reference=TimeDimensionReference(element_name=METRIC_TIME_ELEMENT_NAME),
                entity_links=(),
                custom_granularities=self._manifest_lookup.custom_granularities,
            )
        )
        self._query_items_analysis_cache: LruCache[
            Tuple[ResolverInputForQuery, MetricReference], QueryItemsAnalysis
        ] = LruCache(128)

    def _get_query_items_analysis(
        self, query_resolver_input: ResolverInputForQuery, metric_reference: MetricReference
    ) -> QueryItemsAnalysis:
        cache_key = (query_resolver_input, metric_reference)
        result = self._query_items_analysis_cache.get(cache_key)
        if result is not None:
            return result
        result = self._uncached_query_items_analysis(query_resolver_input, metric_reference)
        self._query_items_analysis_cache.set(cache_key, result)
        return result

    def _uncached_query_items_analysis(
        self, query_resolver_input: ResolverInputForQuery, metric_reference: MetricReference
    ) -> QueryItemsAnalysis:
        has_agg_time_dimension = False
        has_metric_time = False
        scds: List[InstanceSpec] = []

        valid_agg_time_dimension_specs = self._manifest_lookup.metric_lookup.get_valid_agg_time_dimensions_for_metric(
            metric_reference
        )

        scd_specs = self._manifest_lookup.metric_lookup.get_joinable_scd_specs_for_metric(metric_reference)

        for group_by_item_input in query_resolver_input.group_by_item_inputs:
            if group_by_item_input.spec_pattern.matches_any(self._metric_time_specs):
                has_metric_time = True

            if group_by_item_input.spec_pattern.matches_any(valid_agg_time_dimension_specs):
                has_agg_time_dimension = True

            scd_matches = group_by_item_input.spec_pattern.match(scd_specs)
            scds.extend(scd_matches)

        return QueryItemsAnalysis(
            scds=scds,
            has_metric_time=has_metric_time,
            has_agg_time_dimension=has_agg_time_dimension,
        )

    def _validate_cumulative_metric(
        self,
        metric_reference: MetricReference,
        metric: Metric,
        query_items_analysis: QueryItemsAnalysis,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> Sequence[MetricFlowQueryResolutionIssue]:
        if (
            metric.type_params is not None
            and metric.type_params.cumulative_type_params is not None
            and (
                metric.type_params.cumulative_type_params.window is not None
                or metric.type_params.cumulative_type_params.grain_to_date is not None
            )
            and not (query_items_analysis.has_metric_time or query_items_analysis.has_agg_time_dimension)
        ):
            return (
                CumulativeMetricRequiresMetricTimeIssue.from_parameters(
                    metric_reference=metric_reference,
                    query_resolution_path=resolution_path,
                ),
            )
        return ()

    def _validate_derived_metric(
        self,
        metric_reference: MetricReference,
        metric: Metric,
        resolution_path: MetricFlowQueryResolutionPath,
        query_items_analysis: QueryItemsAnalysis,
    ) -> Sequence[MetricFlowQueryResolutionIssue]:
        has_time_offset = any(
            input_metric.offset_window is not None or input_metric.offset_to_grain is not None
            for input_metric in metric.input_metrics
        )

        if has_time_offset and not (
            query_items_analysis.has_metric_time or query_items_analysis.has_agg_time_dimension
        ):
            return (
                OffsetMetricRequiresMetricTimeIssue.from_parameters(
                    metric_reference=metric_reference,
                    input_metrics=metric.input_metrics,
                    query_resolution_path=resolution_path,
                ),
            )
        return ()

    @override
    def validate_metric_in_resolution_dag(
        self,
        metric_reference: MetricReference,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        metric = self._manifest_lookup.metric_lookup.get_metric(metric_reference)

        query_items_analysis = self._get_query_items_analysis(self._resolver_input_for_query, metric_reference)

        issues: List[MetricFlowQueryResolutionIssue] = []

        # Queries that join to an SCD don't support direct references to agg_time_dimension, so we
        # only check for metric_time. If we decide to support agg_time_dimension, we should add a check
        if len(query_items_analysis.scds) > 0 and not query_items_analysis.has_metric_time:
            issues.append(
                ScdRequiresMetricTimeIssue.from_parameters(
                    scds_in_query=query_items_analysis.scds,
                    query_resolution_path=resolution_path,
                )
            )

        if metric.type is MetricType.CUMULATIVE:
            issues.extend(
                self._validate_cumulative_metric(
                    metric_reference=metric_reference,
                    metric=metric,
                    query_items_analysis=query_items_analysis,
                    resolution_path=resolution_path,
                )
            )

        elif metric.type is MetricType.RATIO or metric.type is MetricType.DERIVED:
            issues.extend(
                self._validate_derived_metric(
                    metric_reference=metric_reference,
                    metric=metric,
                    query_items_analysis=query_items_analysis,
                    resolution_path=resolution_path,
                )
            )
        elif metric.type is MetricType.SIMPLE:
            pass
        elif metric.type is MetricType.CONVERSION:
            pass
        else:
            assert_values_exhausted(metric.type)

        return MetricFlowQueryResolutionIssueSet(issues=tuple(issues))

    @override
    def validate_query_in_resolution_dag(
        self,
        metrics_in_query: Sequence[MetricReference],
        where_filter_intersection: WhereFilterIntersection,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        return MetricFlowQueryResolutionIssueSet.empty_instance()
