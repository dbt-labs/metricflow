from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Sequence, Tuple

from typing_extensions import override

from metricflow_semantics.helpers.string_helpers import mf_indent
from metricflow_semantics.mf_logging.pretty_print import PrettyFormatDictOption, mf_pformat
from metricflow_semantics.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow_semantics.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
)
from metricflow_semantics.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NoMatchingItemsForMeasure(MetricFlowQueryResolutionIssue):
    """Describes an issue with the query where there are no matching items for a measure.

    This can happen if the user specifies a group-by-item that does not exist or is not available for the measure.
    """

    suggestions: Tuple[str, ...]

    @staticmethod
    def from_parameters(  # noqa: D102
        parent_issues: Sequence[MetricFlowQueryResolutionIssue],
        query_resolution_path: MetricFlowQueryResolutionPath,
        input_suggestions: Sequence[str],
    ) -> NoMatchingItemsForMeasure:
        return NoMatchingItemsForMeasure(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=tuple(parent_issues),
            query_resolution_path=query_resolution_path,
            suggestions=tuple(input_suggestions),
        )

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        lines = [
            f"The given input does not match any of the available group-by-items for\n"
            f"{self.query_resolution_path.last_item.ui_description}. Common issues are:\n",
            mf_indent(
                "* Incorrect names.\n"
                "* No valid join paths exist from the measure to the group-by-item.\n"
                "  (fan-out join support is pending).\n"
                "* There are multiple matching join paths.\n"
                "  (disambiguation support is pending)."
            ),
        ]

        if len(self.suggestions) > 0:
            lines.append(
                f"\nSuggestions:\n{mf_indent(mf_pformat(list(self.suggestions), format_option=PrettyFormatDictOption(max_line_length=80)))}"
            )
        return "\n".join(lines)

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> NoMatchingItemsForMeasure:
        return NoMatchingItemsForMeasure(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix),
            suggestions=self.suggestions,
        )
