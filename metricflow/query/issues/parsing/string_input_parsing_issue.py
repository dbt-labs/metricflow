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
class StringInputParsingIssue(MetricFlowQueryResolutionIssue):
    """Describe an issue with the query where the input string doesn't match one of the known naming schemes."""

    input_str: str

    @staticmethod
    def from_parameters(input_str: str) -> StringInputParsingIssue:  # noqa: D
        return StringInputParsingIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=(),
            query_resolution_path=MetricFlowQueryResolutionPath.empty_instance(),
            input_str=input_str,
        )

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        return f"The input {repr(self.input_str)} does not match any of the known formats."

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> StringInputParsingIssue:
        return StringInputParsingIssue(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix),
            input_str=self.input_str,
        )
