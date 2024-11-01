from __future__ import annotations

import typing
from typing import List, Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols import Metric, WhereFilterIntersection
from dbt_semantic_interfaces.references import (
    MeasureReference,
    MetricReference,
)
from dbt_semantic_interfaces.type_enums import MetricType
from typing_extensions import override

from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_element_set import LinkableElementSet
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
from metricflow_semantics.query.resolver_inputs.query_resolver_inputs import ResolverInputForQuery
from metricflow_semantics.query.validation_rules.base_validation_rule import PostResolutionQueryValidationRule

if typing.TYPE_CHECKING:
    from metricflow_semantics.query.query_resolver import ResolveGroupByItemsResult


class MetricTimeQueryValidationRule(PostResolutionQueryValidationRule):
    """Validates cases where a query requires metric_time to be specified as a group-by-item.

    Currently, known cases are:

    * Cumulative metrics.
    * Derived metrics with an offset time.
    * Slowly changing dimensions
    """

    def __init__(  # noqa: D107
        self,
        manifest_lookup: SemanticManifestLookup,
        resolver_input_for_query: ResolverInputForQuery,
        resolve_group_by_item_result: ResolveGroupByItemsResult,
    ) -> None:
        super().__init__(
            manifest_lookup=manifest_lookup,
            resolver_input_for_query=resolver_input_for_query,
            resolve_group_by_item_result=resolve_group_by_item_result,
        )

        self._query_includes_metric_time = (
            self._resolve_group_by_item_result.linkable_element_set.filter(
                LinkableElementFilter(with_any_of=frozenset({LinkableElementProperty.METRIC_TIME}))
            ).spec_count
            > 0
        )

        self._scd_linkable_element_set = self._resolve_group_by_item_result.linkable_element_set.filter(
            LinkableElementFilter(with_any_of=frozenset({LinkableElementProperty.SCD_HOP}))
        )

    def _query_includes_agg_time_dimension_of_metric(self, metric_reference: MetricReference) -> bool:
        valid_agg_time_dimensions = self._manifest_lookup.metric_lookup.get_valid_agg_time_dimensions_for_metric(
            metric_reference
        )
        return (
            len(set(valid_agg_time_dimensions).intersection(self._resolve_group_by_item_result.group_by_item_specs)) > 0
        )

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

    def _scd_linkable_element_set_for_measure(self, measure_reference: MeasureReference) -> LinkableElementSet:
        """Returns subset of the query's `LinkableElements` that are SCDs and associated with the measure."""
        measure_semantic_model = self._manifest_lookup.semantic_model_lookup.measure_lookup.get_properties(
            measure_reference
        ).model_reference

        return self._scd_linkable_element_set.filter_by_left_semantic_model(measure_semantic_model)

    @override
    def validate_metric_in_resolution_dag(
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
    def validate_measure_in_resolution_dag(
        self,
        measure_reference: MeasureReference,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        scd_linkable_elemenent_set_for_measure = self._scd_linkable_element_set_for_measure(measure_reference)

        if scd_linkable_elemenent_set_for_measure.spec_count == 0:
            return MetricFlowQueryResolutionIssueSet.empty_instance()

        if self._query_includes_metric_time:
            return MetricFlowQueryResolutionIssueSet.empty_instance()

        # Queries that join to an SCD don't support direct references to agg_time_dimension, so we
        # only check for metric_time. If we decide to support agg_time_dimension, we should add a check

        return MetricFlowQueryResolutionIssueSet.from_issue(
            ScdRequiresMetricTimeIssue.from_parameters(
                scds_in_query=scd_linkable_elemenent_set_for_measure.specs, query_resolution_path=resolution_path
            )
        )
