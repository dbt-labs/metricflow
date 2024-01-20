from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, Tuple

from typing_extensions import override

from metricflow.mf_logging.formatting import indent
from metricflow.mf_logging.pretty_print import mf_pformat
from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
)
from metricflow.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput


@dataclass(frozen=True)
class InvalidMetricIssue(MetricFlowQueryResolutionIssue):
    """Describes when a metric specified as an input to a query does not match any of the known metrics."""

    metric_suggestions: Tuple[str, ...]

    @staticmethod
    def from_parameters(  # noqa: D
        metric_suggestions: Sequence[str],
        query_resolution_path: MetricFlowQueryResolutionPath,
    ) -> InvalidMetricIssue:
        return InvalidMetricIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=(),
            metric_suggestions=tuple(metric_suggestions),
            query_resolution_path=query_resolution_path,
        )

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        return (
            f"The given input does not exactly match any known metrics.\n\n"
            f"Suggestions:\n"
            f"{indent(mf_pformat(list(self.metric_suggestions)))}"
        )

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> InvalidMetricIssue:
        return InvalidMetricIssue(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix),
            metric_suggestions=self.metric_suggestions,
        )
