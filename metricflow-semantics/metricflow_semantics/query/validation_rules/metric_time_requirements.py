from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import MetricReference, TimeDimensionReference
from dbt_semantic_interfaces.type_enums import MetricType
from typing_extensions import override

from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow_semantics.query.issues.issues_base import MetricFlowQueryResolutionIssueSet
from metricflow_semantics.query.issues.parsing.cumulative_metric_requires_metric_time import (
    CumulativeMetricRequiresMetricTimeIssue,
)
from metricflow_semantics.query.issues.parsing.offset_metric_requires_metric_time import (
    OffsetMetricRequiresMetricTimeIssue,
)
from metricflow_semantics.query.resolver_inputs.query_resolver_inputs import ResolverInputForQuery
from metricflow_semantics.query.validation_rules.base_validation_rule import PostResolutionQueryValidationRule
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec


@dataclass(frozen=True)
class QueryItemsAnalysis:
    """Contains data about which items a query contains."""

    has_metric_time: bool
    has_agg_time_dimension: bool


class MetricTimeQueryValidationRule(PostResolutionQueryValidationRule):
    """Validates cases where a query requires metric_time to be specified as a group-by-item.

    Currently, known cases are:

    * Cumulative metrics.
    * Derived metrics with an offset time.g
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

    @lru_cache
    def _get_query_items_analysis(
        self, query_resolver_input: ResolverInputForQuery, metric_reference: MetricReference
    ) -> QueryItemsAnalysis:
        has_agg_time_dimension = False
        has_metric_time = False

        valid_agg_time_dimension_specs = self._manifest_lookup.metric_lookup.get_valid_agg_time_dimensions_for_metric(
            metric_reference
        )
        for group_by_item_input in query_resolver_input.group_by_item_inputs:
            if group_by_item_input.spec_pattern.matches_any(self._metric_time_specs):
                has_metric_time = True

            if group_by_item_input.spec_pattern.matches_any(valid_agg_time_dimension_specs):
                has_agg_time_dimension = True

        return QueryItemsAnalysis(
            has_metric_time=has_metric_time,
            has_agg_time_dimension=has_agg_time_dimension,
        )

    @override
    def validate_metric_in_resolution_dag(
        self,
        metric_reference: MetricReference,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        metric = self._manifest_lookup.metric_lookup.get_metric(metric_reference)

        query_items_analysis = self._get_query_items_analysis(self._resolver_input_for_query, metric_reference)

        issues = MetricFlowQueryResolutionIssueSet.empty_instance()

        if metric.type is MetricType.CUMULATIVE:
            if (
                metric.type_params is not None
                and metric.type_params.cumulative_type_params is not None
                and (
                    metric.type_params.cumulative_type_params.window is not None
                    or metric.type_params.cumulative_type_params.grain_to_date is not None
                )
                and not (query_items_analysis.has_metric_time or query_items_analysis.has_agg_time_dimension)
            ):
                issues = issues.add_issue(
                    CumulativeMetricRequiresMetricTimeIssue.from_parameters(
                        metric_reference=metric_reference,
                        query_resolution_path=resolution_path,
                    )
                )
        elif metric.type is MetricType.RATIO or metric.type is MetricType.DERIVED:
            has_time_offset = any(
                input_metric.offset_window is not None or input_metric.offset_to_grain is not None
                for input_metric in metric.input_metrics
            )

            if has_time_offset and not (
                query_items_analysis.has_metric_time or query_items_analysis.has_agg_time_dimension
            ):
                issues = issues.add_issue(
                    OffsetMetricRequiresMetricTimeIssue.from_parameters(
                        metric_reference=metric_reference,
                        input_metrics=metric.input_metrics,
                        query_resolution_path=resolution_path,
                    )
                )
        elif metric.type is not MetricType.SIMPLE and metric.type is not MetricType.CONVERSION:
            assert_values_exhausted(metric.type)

        return issues

    @override
    def validate_query_in_resolution_dag(
        self,
        metrics_in_query: Sequence[MetricReference],
        where_filter_intersection: WhereFilterIntersection,
        resolution_path: MetricFlowQueryResolutionPath,
    ) -> MetricFlowQueryResolutionIssueSet:
        return MetricFlowQueryResolutionIssueSet.empty_instance()


__all__ = ["MetricTimeQueryValidationRule"]
