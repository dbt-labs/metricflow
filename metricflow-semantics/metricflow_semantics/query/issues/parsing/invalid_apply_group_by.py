from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from typing_extensions import override

from metricflow_semantics.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow_semantics.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
)
from metricflow_semantics.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput
from metricflow_semantics.query.resolver_inputs.query_resolver_inputs import (
    ResolverInputForMetric,
)


@dataclass(frozen=True)
class InvalidApplyGroupByIssue(MetricFlowQueryResolutionIssue):
    """Describes an issue with the query where the apply group by param is invalid."""

    apply_group_by: bool
    metric_inputs: Tuple[ResolverInputForMetric, ...]

    @staticmethod
    def from_parameters(  # noqa: D102
        apply_group_by: bool,
        metric_inputs: Tuple[ResolverInputForMetric, ...],
        query_resolution_path: MetricFlowQueryResolutionPath,
    ) -> InvalidApplyGroupByIssue:
        return InvalidApplyGroupByIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=(),
            query_resolution_path=query_resolution_path,
            apply_group_by=apply_group_by,
            metric_inputs=metric_inputs,
        )

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        return "`apply_group_by` cannot be False for a query with metrics."

    @override
    def with_path_prefix(self, path_prefix_node: MetricFlowQueryResolutionPath) -> InvalidApplyGroupByIssue:
        return InvalidApplyGroupByIssue(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix_node) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix_node),
            apply_group_by=self.apply_group_by,
            metric_inputs=self.metric_inputs,
        )
