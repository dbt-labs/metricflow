from __future__ import annotations

from dataclasses import dataclass
from typing import List

from typing_extensions import override

from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.formatting import indent_log_line
from metricflow.naming.object_builder_scheme import ObjectBuilderNamingScheme
from metricflow.query.group_by_item.candidate_push_down.group_by_item_candidate import GroupByItemCandidateSet
from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
)
from metricflow.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput


@dataclass(frozen=True)
class AmbiguousGroupByItemIssue(MetricFlowQueryResolutionIssue):
    """Describes an issue with the query where the input is ambiguous and it can't be resolved."""

    candidate_set: GroupByItemCandidateSet

    @staticmethod
    def from_parameters(  # noqa: D
        candidate_set: GroupByItemCandidateSet,
        query_resolution_path: MetricFlowQueryResolutionPath,
    ) -> AmbiguousGroupByItemIssue:
        return AmbiguousGroupByItemIssue(
            issue_type=MetricFlowQueryIssueType.ERROR,
            parent_issues=(),
            candidate_set=candidate_set,
            query_resolution_path=query_resolution_path,
        )

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        if associated_input.input_pattern_description is not None:
            naming_scheme = associated_input.input_pattern_description.naming_scheme
        else:
            naming_scheme = ObjectBuilderNamingScheme()
        candidates_str: List[str] = []

        for spec in self.candidate_set.specs:
            input_str = naming_scheme.input_str(spec)
            if input_str is not None:
                candidates_str.append(input_str)
            else:
                candidates_str.append(str(spec))
        return (
            f"The given input is ambiguous and can't be resolved. The input could match:\n\n"
            f"{indent_log_line(mf_pformat(candidates_str))}"
        )

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> AmbiguousGroupByItemIssue:
        return AmbiguousGroupByItemIssue(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix),
            candidate_set=self.candidate_set,
        )
