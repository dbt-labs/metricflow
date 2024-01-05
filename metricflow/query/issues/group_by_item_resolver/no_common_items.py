from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Sequence, Tuple

from typing_extensions import override

from metricflow.mf_logging.formatting import indent
from metricflow.mf_logging.pretty_print import mf_pformat
from metricflow.naming.object_builder_scheme import ObjectBuilderNamingScheme
from metricflow.query.group_by_item.candidate_push_down.group_by_item_candidate import GroupByItemCandidateSet
from metricflow.query.group_by_item.resolution_dag.resolution_nodes.base_node import GroupByItemResolutionNode
from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.issues.issues_base import (
    MetricFlowQueryIssueType,
    MetricFlowQueryResolutionIssue,
)
from metricflow.query.resolver_inputs.base_resolver_inputs import MetricFlowQueryResolverInput


@dataclass(frozen=True)
class NoCommonItemsInParents(MetricFlowQueryResolutionIssue):
    """Describes an issue during group-by-item resolution where parents don't have common items that match a pattern.

    Currently, this only occurs when resolving a time dimension specified without a grain in a where filter. In such
    cases, only the time dimension at the defined grain is available from parents. This is how we defined how such
    ambiguous time dimensions should be resolved.

    e.g. a query for a daily metric and a monthly metric would consist of a query node with the daily metric node and
    the monthly metric node as the parents.

    To resolve a query filter like "{{ TimeDimension('metric_time') }} = '2020-01-01'", the query node would receive
    metric_time__day from the daily metric node and metric_time__month from the monthly metric node. Then this issue
    would be created to let the user know that the ambiguous group-by-item in the where filter can't be resolved.
    """

    parent_candidate_sets: Tuple[GroupByItemCandidateSet, ...]

    @staticmethod
    def from_parameters(  # noqa: D
        query_resolution_path: MetricFlowQueryResolutionPath,
        parent_node_to_candidate_set: Dict[GroupByItemResolutionNode, GroupByItemCandidateSet],
        parent_issues: Sequence[MetricFlowQueryResolutionIssue],
    ) -> NoCommonItemsInParents:
        return NoCommonItemsInParents(
            issue_type=MetricFlowQueryIssueType.ERROR,
            query_resolution_path=query_resolution_path,
            parent_candidate_sets=tuple(candidate_set for _, candidate_set in parent_node_to_candidate_set.items()),
            parent_issues=tuple(parent_issues),
        )

    @override
    def ui_description(self, associated_input: MetricFlowQueryResolverInput) -> str:
        last_path_item = self.query_resolution_path.last_item
        last_path_item_parent_descriptions = ", ".join(
            [parent_node.ui_description for parent_node in last_path_item.parent_nodes]
        )
        naming_scheme = (
            associated_input.input_pattern_description.naming_scheme
            if associated_input.input_pattern_description is not None
            else ObjectBuilderNamingScheme()
        )

        parent_to_available_items = {}
        for candidate_set in self.parent_candidate_sets:
            resolution_node = candidate_set.path_from_leaf_node.last_item
            spec_as_strs = tuple(naming_scheme.input_str(spec) for spec in candidate_set.specs)
            parent_to_available_items["Matching items for: " + resolution_node.ui_description] = [
                (spec_str if spec_str is not None else "None") for spec_str in spec_as_strs
            ]
        return (
            f"{last_path_item.ui_description} is built from:\n\n"
            f"{indent(last_path_item_parent_descriptions)}.\n"
            f"However, the given input does not match to a common item that is available to those parents:\n\n"
            f"{indent(mf_pformat(parent_to_available_items, max_line_length=80))}\n\n"
            f"For time dimension inputs, please specify a time grain as ambiguous resolution only allows "
            f"resolution when the parents have the same defined time gain."
        )

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> NoCommonItemsInParents:
        return NoCommonItemsInParents(
            issue_type=self.issue_type,
            parent_issues=tuple(issue.with_path_prefix(path_prefix) for issue in self.parent_issues),
            query_resolution_path=self.query_resolution_path.with_path_prefix(path_prefix),
            parent_candidate_sets=tuple(
                candidate_set.with_path_prefix(path_prefix) for candidate_set in self.parent_candidate_sets
            ),
        )
