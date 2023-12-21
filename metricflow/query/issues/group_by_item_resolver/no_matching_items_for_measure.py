from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Sequence, Tuple

from typing_extensions import override

from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.formatting import indent_log_line
from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
)
from metricflow.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NoMatchingItemsForMeasure(MetricFlowQueryResolutionIssue):
    """Describes an issue with the query where there are no matching items for a measure.

    This can happen if the user specifies a group-by-item that does not exist or is not available for the measure.
    """

    suggestions: Tuple[str, ...]

    @staticmethod
    def from_parameters(  # noqa: D
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
            f"The given input does not match any of the available group-by-items for "
            f"{self.query_resolution_path.last_item.ui_description}. Common issues are:\n",
            indent_log_line(
                "* Incorrect names.\n"
                "* No valid join paths exist from the measure to the group-by-item "
                "(fan-out joins are not yet supported).\n"
                "* There are multiple matching join paths (disambiguation support is pending)."
            ),
        ]

        if len(self.suggestions) > 0:
            lines.append(f"\nSuggestions:\n{indent_log_line(mf_pformat(list(self.suggestions)))}")
        return "\n".join(lines)

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> NoMatchingItemsForMeasure:
        return NoMatchingItemsForMeasure(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix),
            suggestions=self.suggestions,
        )
