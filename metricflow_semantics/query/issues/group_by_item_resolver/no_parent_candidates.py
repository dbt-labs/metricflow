from __future__ import annotations

from dataclasses import dataclass

from typing_extensions import override

from metricflow_semantics.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow_semantics.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
)
from metricflow_semantics.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput


@dataclass(frozen=True)
class NoParentCandidates(MetricFlowQueryResolutionIssue):
    """Describes an issue during group-by-item resolution where parents don't have any candidates that match a pattern.

    This most likely indicates a problem with either the query or the model where a given input is supposed to have
    parent nodes, but does not. For example, a derived metric with no defined metrics or measures as inputs would not
    be resolvable since it requires parent resolution, but no parents are given. While these issues should be caught
    in validation, they may slip through the cracks, and so we raise a more helpful user-facing error in these cases.
    """

    @staticmethod
    def from_parameters(  # noqa: D102
        query_resolution_path: MetricFlowQueryResolutionPath,
    ) -> NoParentCandidates:
        return NoParentCandidates(
            issue_type=MetricFlowQueryIssueType.ERROR,
            query_resolution_path=query_resolution_path,
            parent_issues=tuple(),
        )

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        last_path_item = self.query_resolution_path.last_item

        return (
            f"{last_path_item.ui_description} is a node that requires certain inputs, but none were specified.\n\n"
            "This most likely indicates a problem with the semantic manifest, such as a derived metric with no "
            "inputs, or else a missing query input parameter. If this proves to be the case, please open an issue "
            "for us to improve our input validation processes."
        )

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> NoParentCandidates:
        return NoParentCandidates(
            issue_type=self.issue_type,
            parent_issues=tuple(),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix),
        )
