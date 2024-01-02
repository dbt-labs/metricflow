from __future__ import annotations

from dataclasses import dataclass

from typing_extensions import override

from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
)
from metricflow.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput


@dataclass(frozen=True)
class InvalidLimitIssue(MetricFlowQueryResolutionIssue):
    """Describes an issue with the query where the limit is invalid."""

    limit: int

    @staticmethod
    def from_parameters(  # noqa: D
        limit: int, query_resolution_path: MetricFlowQueryResolutionPath
    ) -> InvalidLimitIssue:
        return InvalidLimitIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=(),
            query_resolution_path=query_resolution_path,
            limit=limit,
        )

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        return f"The limit {repr(self.limit)} is not >= 0."

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> InvalidLimitIssue:
        return InvalidLimitIssue(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix),
            limit=self.limit,
        )
