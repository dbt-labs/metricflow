from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from typing_extensions import override

from metricflow_semantics.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow_semantics.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
)
from metricflow_semantics.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput


@dataclass(frozen=True)
class ScdRequiresMetricTimeIssue(MetricFlowQueryResolutionIssue):
    """Describes an issue with a query that includes a SCD group by but does not include metric_time."""

    scd_qualified_names: Sequence[str]

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        dim_str = ", ".join(self.scd_qualified_names)
        return (
            f"Your query contains the Slowly Changing Dimensions (SCDs): [{dim_str}]. "
            "A query containing SCDs must also contain the metric_time dimension in order "
            "to join the SCD table to the valid time range. Please add metric_time "
            "to the query and try again. If you're using agg_time_dimension, use "
            "metric_time instead."
        )

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> ScdRequiresMetricTimeIssue:
        return ScdRequiresMetricTimeIssue(
            issue_type=self.issue_type,
            parent_issues=self.parent_issues,
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix),
            scd_qualified_names=self.scd_qualified_names,
        )

    @staticmethod
    def from_parameters(  # noqa: D102
        scd_qualified_names: Sequence[str], query_resolution_path: MetricFlowQueryResolutionPath
    ) -> ScdRequiresMetricTimeIssue:
        return ScdRequiresMetricTimeIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=(),
            query_resolution_path=query_resolution_path,
            scd_qualified_names=scd_qualified_names,
        )
