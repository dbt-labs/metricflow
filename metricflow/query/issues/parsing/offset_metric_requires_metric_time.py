from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, Tuple

from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.protocols import MetricInput
from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import override

from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
)
from metricflow.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput


@dataclass(frozen=True)
class OffsetMetricRequiresMetricTimeIssue(MetricFlowQueryResolutionIssue):
    """Describes an issue where metric_time is required, but not included in the query.

    metric_time is required as a group-by-item when querying offset metrics.
    """

    metric_reference: MetricReference
    input_metrics: Tuple[MetricInput, ...]

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        return (
            f"The query includes a metric {repr(self.metric_reference.element_name)} that specifies a time offset in "
            f"input metrics: {repr(self.input_metrics)}. However, group-by-items do not include "
            f"{repr(METRIC_TIME_ELEMENT_NAME)}."
        )

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> OffsetMetricRequiresMetricTimeIssue:
        return OffsetMetricRequiresMetricTimeIssue(
            issue_type=self.issue_type,
            parent_issues=self.parent_issues,
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix),
            metric_reference=self.metric_reference,
            input_metrics=self.input_metrics,
        )

    @staticmethod
    def from_parameters(  # noqa: D
        metric_reference: MetricReference,
        input_metrics: Sequence[MetricInput],
        query_resolution_path: MetricFlowQueryResolutionPath,
    ) -> OffsetMetricRequiresMetricTimeIssue:
        return OffsetMetricRequiresMetricTimeIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=(),
            query_resolution_path=query_resolution_path,
            metric_reference=metric_reference,
            input_metrics=tuple(input_metrics),
        )
