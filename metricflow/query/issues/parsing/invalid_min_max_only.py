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
class InvalidMinMaxOnlyIssue(MetricFlowQueryResolutionIssue):
    """Describes an issue with the query where the limit is invalid."""

    min_max_only: bool

    @staticmethod
    def from_parameters(  # noqa: D
        min_max_only: bool, query_resolution_path: MetricFlowQueryResolutionPath
    ) -> InvalidMinMaxOnlyIssue:
        return InvalidMinMaxOnlyIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=(),
            query_resolution_path=query_resolution_path,
            min_max_only=min_max_only,
        )

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        return "`min_max_only` must be used with exactly one `group_by`, and cannot be used with `metrics`, `order_by`, or `limit`."

    @override
    def with_path_prefix(self, path_prefix_node: MetricFlowQueryResolutionPath) -> InvalidMinMaxOnlyIssue:
        return InvalidMinMaxOnlyIssue(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix_node) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix_node),
            min_max_only=self.min_max_only,
        )
