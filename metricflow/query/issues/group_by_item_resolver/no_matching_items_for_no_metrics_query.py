from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from typing_extensions import override

from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
)
from metricflow.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput


@dataclass(frozen=True)
class NoMatchingItemsForNoMetricsQuery(MetricFlowQueryResolutionIssue):
    """Describes an issue with the query where there are no matching items for a no-metrics / distinct values query."""

    @staticmethod
    def from_parameters(  # noqa: D
        parent_issues: Sequence[MetricFlowQueryResolutionIssue],
        query_resolution_path: MetricFlowQueryResolutionPath,
    ) -> NoMatchingItemsForNoMetricsQuery:
        return NoMatchingItemsForNoMetricsQuery(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=tuple(parent_issues),
            query_resolution_path=query_resolution_path,
        )

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        # Could be useful to list the candidates, but it makes the logs very long.
        return (
            "The given input does not match any of the available group-by-items for a distinct\n"
            "values query without metrics."
        )

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> NoMatchingItemsForNoMetricsQuery:
        return NoMatchingItemsForNoMetricsQuery(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix),
        )
