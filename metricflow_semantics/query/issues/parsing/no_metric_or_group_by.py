from __future__ import annotations

from dataclasses import dataclass

from typing_extensions import override

from metricflow_semantics.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow_semantics.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
)
from metricflow_semantics.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput
from metricflow_semantics.query.resolver_inputs.query_resolver_inputs import ResolverInputForQuery


@dataclass(frozen=True)
class NoMetricOrGroupByIssue(MetricFlowQueryResolutionIssue):
    """Describes an issue with the query where there are no metrics or group by inputs in the query."""

    resolver_input_for_query: ResolverInputForQuery

    @staticmethod
    def from_parameters(  # noqa: D102
        resolver_input_for_query: ResolverInputForQuery, query_resolution_path: MetricFlowQueryResolutionPath
    ) -> NoMetricOrGroupByIssue:
        return NoMetricOrGroupByIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=(),
            resolver_input_for_query=resolver_input_for_query,
            query_resolution_path=query_resolution_path,
        )

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        return "There are no metrics or group by items requested in the query."

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> NoMetricOrGroupByIssue:
        return NoMetricOrGroupByIssue(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix) for issue in self.parent_issues),
            resolver_input_for_query=self.resolver_input_for_query,
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix),
        )
