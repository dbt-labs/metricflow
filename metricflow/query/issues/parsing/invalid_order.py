from __future__ import annotations

from dataclasses import dataclass

from typing_extensions import override

from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
)
from metricflow.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput
from metricflow.query.resolver_inputs.query_resolver_inputs import (
    ResolverInputForOrderByItem,
)


@dataclass(frozen=True)
class InvalidOrderByItemIssue(MetricFlowQueryResolutionIssue):
    """Describes a query issue where the order-by item does not match one of the queried metrics / group-by-items."""

    order_by_item_input: ResolverInputForOrderByItem

    @staticmethod
    def from_parameters(  # noqa: D
        order_by_item_input: ResolverInputForOrderByItem,
        query_resolution_path: MetricFlowQueryResolutionPath,
    ) -> InvalidOrderByItemIssue:
        return InvalidOrderByItemIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=(),
            query_resolution_path=query_resolution_path,
            order_by_item_input=order_by_item_input,
        )

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        return (
            f"The order-by item {repr(self.order_by_item_input.input_obj)} does not match exactly one "
            f"of the query items."
        )

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> InvalidOrderByItemIssue:
        return InvalidOrderByItemIssue(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix),
            order_by_item_input=self.order_by_item_input,
        )
