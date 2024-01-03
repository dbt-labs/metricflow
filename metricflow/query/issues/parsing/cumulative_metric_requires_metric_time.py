from __future__ import annotations

from dataclasses import dataclass

from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import override

from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
)
from metricflow.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput


@dataclass(frozen=True)
class CumulativeMetricRequiresMetricTimeIssue(MetricFlowQueryResolutionIssue):
    """Describes an issue with a query that includes a cumulative metric but does not include metric_time."""

    metric_reference: MetricReference

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        return (
            f"The query includes a cumulative metric {repr(self.metric_reference.element_name)} that does not "
            f"accumulate over all-time, but the group-by items do not include {repr(METRIC_TIME_ELEMENT_NAME)}"
        )

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> CumulativeMetricRequiresMetricTimeIssue:
        return CumulativeMetricRequiresMetricTimeIssue(
            issue_type=self.issue_type,
            parent_issues=self.parent_issues,
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix),
            metric_reference=self.metric_reference,
        )

    @staticmethod
    def from_parameters(  # noqa: D
        metric_reference: MetricReference, query_resolution_path: MetricFlowQueryResolutionPath
    ) -> CumulativeMetricRequiresMetricTimeIssue:
        return CumulativeMetricRequiresMetricTimeIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=(),
            query_resolution_path=query_resolution_path,
            metric_reference=metric_reference,
        )
