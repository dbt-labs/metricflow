from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from typing_extensions import override

from metricflow.mf_logging.formatting import indent
from metricflow.mf_logging.pretty_print import mf_pformat
from metricflow.naming.object_builder_scheme import ObjectBuilderNamingScheme
from metricflow.query.group_by_item.candidate_push_down.group_by_item_candidate import GroupByItemCandidateSet
from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
)
from metricflow.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput


@dataclass(frozen=True)
class MultipleMatchIssue(MetricFlowQueryResolutionIssue):
    """Describes an issue during group-by-item resolution the input pattern matches multiple specs."""

    candidate_set: GroupByItemCandidateSet

    @staticmethod
    def from_parameters(  # noqa: D
        query_resolution_path: MetricFlowQueryResolutionPath,
        candidate_set: GroupByItemCandidateSet,
        parent_issues: Sequence[MetricFlowQueryResolutionIssue],
    ) -> MultipleMatchIssue:
        return MultipleMatchIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            query_resolution_path=query_resolution_path,
            candidate_set=candidate_set,
            parent_issues=tuple(parent_issues),
        )

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        last_path_item = self.query_resolution_path.last_item
        naming_scheme = (
            associated_input.input_pattern_description.naming_scheme
            if associated_input.input_pattern_description is not None
            else ObjectBuilderNamingScheme()
        )

        specs_as_strs = []
        for spec in self.candidate_set.specs:
            input_str = naming_scheme.input_str(spec)
            if input_str is not None:
                specs_as_strs.append(input_str)
            else:
                specs_as_strs.append(f"<{repr(spec)}>")

        return (
            f"The given input matches multiple group-by-items for {last_path_item.ui_description}:\n\n"
            f"{indent(mf_pformat(specs_as_strs))}\n\n"
            f"Please use a more specific input to resolve the ambiguity."
        )

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> MultipleMatchIssue:
        return MultipleMatchIssue(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix),
            candidate_set=self.candidate_set.with_path_prefix(path_prefix),
        )
