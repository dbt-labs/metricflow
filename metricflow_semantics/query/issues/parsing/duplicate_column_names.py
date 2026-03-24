from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, Tuple

from typing_extensions import override

from metricflow_semantics.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow_semantics.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
)
from metricflow_semantics.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput


@dataclass(frozen=True)
class DuplicateOutputColumnIssue(MetricFlowQueryResolutionIssue):
    """Describes when there are duplicate output column names in a query."""

    duplicate_column_names: Tuple[str, ...]

    @staticmethod
    def from_parameters(  # noqa: D102
        duplicate_column_names: Sequence[str],
        query_resolution_path: MetricFlowQueryResolutionPath,
    ) -> DuplicateOutputColumnIssue:
        return DuplicateOutputColumnIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=(),
            duplicate_column_names=tuple(duplicate_column_names),
            query_resolution_path=query_resolution_path,
        )

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        return f"Query contains duplicate output column names: {self.duplicate_column_names}"

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> DuplicateOutputColumnIssue:
        return DuplicateOutputColumnIssue(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix),
            duplicate_column_names=self.duplicate_column_names,
        )
