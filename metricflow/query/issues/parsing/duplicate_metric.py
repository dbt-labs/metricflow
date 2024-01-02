from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, Tuple

from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import override

from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
)
from metricflow.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput


@dataclass(frozen=True)
class DuplicateMetricIssue(MetricFlowQueryResolutionIssue):
    """Describes when there are duplicate metrics in a query."""

    duplicate_metric_references: Tuple[MetricReference, ...]

    @staticmethod
    def from_parameters(  # noqa: D
        duplicate_metric_references: Sequence[MetricReference],
        query_resolution_path: MetricFlowQueryResolutionPath,
    ) -> DuplicateMetricIssue:
        return DuplicateMetricIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=(),
            duplicate_metric_references=tuple(duplicate_metric_references),
            query_resolution_path=query_resolution_path,
        )

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        return (
            f"Query contains duplicate metrics: "
            f"{[metric_reference.element_name for metric_reference in self.duplicate_metric_references]}"
        )

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> DuplicateMetricIssue:
        return DuplicateMetricIssue(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix),
            duplicate_metric_references=self.duplicate_metric_references,
        )
