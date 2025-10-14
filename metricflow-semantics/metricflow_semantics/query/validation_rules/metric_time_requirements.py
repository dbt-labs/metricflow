from __future__ import annotations

import typing
from typing import List, Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols import Metric, WhereFilterIntersection
from dbt_semantic_interfaces.references import (
    MetricReference,
)
from dbt_semantic_interfaces.type_enums import MetricType
from typing_extensions import override

from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
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
from metricflow_semantics.query.resolver_inputs.query_resolver_inputs import ResolverInputForQuery
from metricflow_semantics.query.validation_rules.base_validation_rule import PostResolutionQueryValidationRule
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.toolkit.collections.ordered_set import OrderedSet

if typing.TYPE_CHECKING:
    from metricflow_semantics.query.query_resolver import ResolveGroupByItemsResult, ResolveMetricsResult


class MetricTimeQueryValidationRule(PostResolutionQueryValidationRule):
    """Validates cases where a query requires metric_time to be specified as a group-by-item.

    Currently, known cases are:

    * Cumulative metrics.
    * Derived metrics with an offset time.
    """

    def __init__(  # noqa: D107
        self,
        manifest_lookup: SemanticManifestLookup,
        resolver_input_for_query: ResolverInputForQuery,
        resolve_group_by_item_result: ResolveGroupByItemsResult,
        resolve_metric_result: ResolveMetricsResult,
    ) -> None:
        super().__init__(
            manifest_lookup=manifest_lookup,
            resolver_input_for_query=resolver_input_for_query,
            resolve_group_by_item_result=resolve_group_by_item_result,
            resolve_metric_result=resolve_metric_result,
        )

        self._query_includes_metric_time = not self._resolve_group_by_item_result.linkable_element_set.filter(
            GroupByItemSetFilter.create(any_properties_allowlist=(GroupByItemProperty.METRIC_TIME,))
        ).is_empty

    def _query_includes_agg_time_dimension_of_metric(self, metric_reference: MetricReference) -> bool:
        valid_agg_time_dimensions: OrderedSet[
            LinkableInstanceSpec
        ] = self._manifest_lookup.metric_lookup.get_aggregation_time_dimension_specs(metric_reference)
        return len(valid_agg_time_dimensions.intersection(self._resolve_group_by_item_result.group_by_item_specs)) > 0

    def _validate_cumulative_metric(
        self,
        metric_reference: MetricReference,
        metric: Metric,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> Sequence[MetricFlowQueryResolutionIssue]:
        # A cumulative metric with a window or grain-to-date specified requires a `metric-time` or the aggregation time
        # dimension for the metric.
        if (
            metric.type_params is not None
            and metric.type_params.cumulative_type_params is not None
            and (
                metric.type_params.cumulative_type_params.window is not None
                or metric.type_params.cumulative_type_params.grain_to_date is not None
            )
        ):
            if self._query_includes_metric_time or self._query_includes_agg_time_dimension_of_metric(metric_reference):
                return ()

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
    ) -> Sequence[MetricFlowQueryResolutionIssue]:
        has_time_offset = any(
            input_metric.offset_window is not None or input_metric.offset_to_grain is not None
            for input_metric in metric.input_metrics
        )

        # If a derived metric does not define a time offset, then there are no requirements on what's in the group-by
        # items.
        if not has_time_offset:
            return ()

        # If a derived metric has a time offset, then the query needs to include `metric_time` or the aggregation time
        # dimension of a metric.
        if self._query_includes_metric_time or self._query_includes_agg_time_dimension_of_metric(metric_reference):
            return ()

        return (
            OffsetMetricRequiresMetricTimeIssue.from_parameters(
                metric_reference=metric_reference,
                input_metrics=metric.input_metrics,
                query_resolution_path=resolution_path,
            ),
        )

    @override
    def validate_complex_metric_in_resolution_dag(
        self,
        metric_reference: MetricReference,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        metric = self._manifest_lookup.metric_lookup.get_metric(metric_reference)
        issues: List[MetricFlowQueryResolutionIssue] = []

        if metric.type is MetricType.CUMULATIVE:
            issues.extend(
                self._validate_cumulative_metric(
                    metric_reference=metric_reference,
                    metric=metric,
                    resolution_path=resolution_path,
                )
            )

        elif metric.type is MetricType.RATIO or metric.type is MetricType.DERIVED:
            issues.extend(
                self._validate_derived_metric(
                    metric_reference=metric_reference,
                    metric=metric,
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

    @override
    def validate_simple_metric_in_resolution_dag(
        self,
        metric_reference: MetricReference,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        return MetricFlowQueryResolutionIssueSet.empty_instance()
