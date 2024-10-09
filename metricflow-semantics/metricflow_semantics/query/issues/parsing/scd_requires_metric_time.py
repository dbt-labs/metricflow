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
from metricflow_semantics.specs.instance_spec import InstanceSpec


@dataclass(frozen=True)
class ScdRequiresMetricTimeIssue(MetricFlowQueryResolutionIssue):
    """Describes an issue with a query that includes a SCD group by but does not include metric_time."""

    scds_in_query: Sequence[InstanceSpec]

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        dim_str = ", ".join(f"'{scd.qualified_name}'" for scd in self.scds_in_query)
        return (
            "Your query contains the following group bys, which are SCDs or contain SCDs "
            f"in the join path: [{dim_str}].\n\nA query containing SCDs must also contain "
            "the metric_time dimension in order to join the SCD table to the valid time "
            "range. Please add metric_time to the query and try again. If you're "
            "using agg_time_dimension, use metric_time instead."
        )

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> ScdRequiresMetricTimeIssue:
        return ScdRequiresMetricTimeIssue(
            issue_type=self.issue_type,
            parent_issues=self.parent_issues,
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix),
            scds_in_query=self.scds_in_query,
        )

    @staticmethod
    def from_parameters(  # noqa: D102
        scds_in_query: Sequence[InstanceSpec], query_resolution_path: MetricFlowQueryResolutionPath
    ) -> ScdRequiresMetricTimeIssue:
        return ScdRequiresMetricTimeIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=(),
            query_resolution_path=query_resolution_path,
            scds_in_query=scds_in_query,
        )
