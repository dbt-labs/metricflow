from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, FrozenSet, Iterator, List, Optional, Sequence, Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums import MetricType
from typing_extensions import override

from metricflow_semantics.mf_logging.formatting import indent
from metricflow_semantics.mf_logging.pretty_print import mf_pformat, mf_pformat_many
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.group_by_item.candidate_push_down.group_by_item_candidate import GroupByItemCandidateSet
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.base_node import (
    GroupByItemResolutionNode,
    GroupByItemResolutionNodeVisitor,
)
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.measure_source_node import (
    MeasureGroupByItemSourceNode,
)
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.metric_resolution_node import (
    MetricGroupByItemResolutionNode,
)
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.no_metrics_query_source_node import (
    NoMetricsGroupByItemSourceNode,
)
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.query_resolution_node import (
    QueryGroupByItemResolutionNode,
)
from metricflow_semantics.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow_semantics.query.issues.group_by_item_resolver.invalid_use_of_date_part import (
    MetricExcludesDatePartIssue,
)
from metricflow_semantics.query.issues.group_by_item_resolver.no_common_items import NoCommonItemsInParents
from metricflow_semantics.query.issues.group_by_item_resolver.no_matching_items_for_measure import (
    NoMatchingItemsForMeasure,
)
from metricflow_semantics.query.issues.group_by_item_resolver.no_matching_items_for_no_metrics_query import (
    NoMatchingItemsForNoMetricsQuery,
)
from metricflow_semantics.query.issues.group_by_item_resolver.no_parent_candidates import NoParentCandidates
from metricflow_semantics.query.issues.issues_base import (
    MetricFlowQueryResolutionIssueSet,
)
from metricflow_semantics.query.suggestion_generator import QueryItemSuggestionGenerator
from metricflow_semantics.specs.patterns.base_time_grain import BaseTimeGrainPattern
from metricflow_semantics.specs.patterns.none_date_part import NoneDatePartPattern
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PushDownResult:
    """The result that is pushed down from the root nodes to the leaf nodes to resolve ambiguous group-by items."""

    # The set of candidate specs that could match the ambiguous group-by item.
    candidate_set: GroupByItemCandidateSet
    # The issues seen so far while pushing down the result / resolving the ambiguity.
    issue_set: MetricFlowQueryResolutionIssueSet

    def __post_init__(self) -> None:  # noqa: D105
        # If there are no errors, there should be a candidate spec in each candidate set.
        # If there are errors, there shouldn't be any candidate sets.
        assert (not self.issue_set.has_errors and not self.candidate_set.is_empty) or (
            self.issue_set.has_errors and self.candidate_set.is_empty
        ), mf_pformat_many(
            "candidate_set / issue_set mismatch:", {"candidate_set": self.candidate_set, "issue_set": self.issue_set}
        )

    def filter_candidates_by_pattern(self, spec_pattern: SpecPattern) -> PushDownResult:
        """Create a new result where only the candidates matching the pattern are included."""
        return PushDownResult(
            candidate_set=self.candidate_set.filter_candidates_by_pattern(spec_pattern),
            issue_set=self.issue_set,
        )

    def filter_candidates_by_patterns(self, spec_patterns: Sequence[SpecPattern]) -> PushDownResult:
        """Create a new result where only the candidates matching the patterns are included."""
        if len(spec_patterns) == 0:
            return self

        candidate_set = self.candidate_set
        for spec_pattern in spec_patterns:
            candidate_set = candidate_set.filter_candidates_by_pattern(spec_pattern)

        return PushDownResult(
            candidate_set=candidate_set,
            issue_set=self.issue_set,
        )


class DagTraversalPathTracker:
    """Helps track the path traveled by a visitor through the nodes in a group-by-tem resolution DAG."""

    def __init__(self) -> None:  # noqa: D107
        self._current_path: List[GroupByItemResolutionNode] = []

    @contextmanager
    def track_node_visit(self, node: GroupByItemResolutionNode) -> Iterator[MetricFlowQueryResolutionPath]:
        """Context manager used to wrap the visit calls.

        e.g.

            def visit_some_node(self, node) -> Result:
                with track_node_visit(node) as current_traversal_path:
                    # current_traversal_path is the path from the start node to this node.
                # context manager removes this node from current_traversal_path so sibling branches don't include it
        """
        self._current_path.append(node)
        yield MetricFlowQueryResolutionPath(tuple(self._current_path))
        self._current_path.pop(-1)


class _PushDownGroupByItemCandidatesVisitor(GroupByItemResolutionNodeVisitor[PushDownResult]):
    """A visitor that implements the logic for group-by-item resolution using the resolution DAG.

    Please see the behavior at each node for more details. Overall, this visitor pushes candidates that match an
    ambiguous group-by-item pattern from the root nodes (representing measures / the specs available for those
    measures) to the leaf node (the query for metrics / valid group-by items for querying). For nodes with multiple
    parents, the candidates from each parent are intersected and passed to the child node.
    """

    def __init__(
        self,
        manifest_lookup: SemanticManifestLookup,
        suggestion_generator: Optional[QueryItemSuggestionGenerator],
        source_spec_patterns: Sequence[SpecPattern] = (),
        with_any_property: Optional[FrozenSet[LinkableElementProperty]] = None,
        without_any_property: Optional[FrozenSet[LinkableElementProperty]] = None,
    ) -> None:
        """Initializer.

        Args:
            manifest_lookup: The semantic manifest lookup associated with the resolution DAG that this will traverse.
            suggestion_generator: If there are issues with matching patterns to specs, use this to generate suggestions
            that will go in the issue.
            source_spec_patterns: The patterns to apply to the specs available at the measure nodes.
            with_any_property: Only consider group-by-items with these properties from the measure nodes.
            without_any_property:  Only consider group-by-items without any of these properties (see
            LinkableElementProperty).
        """
        self._semantic_manifest_lookup = manifest_lookup
        self._source_spec_patterns = tuple(source_spec_patterns)
        self._path_from_start_node_tracker = DagTraversalPathTracker()
        self._with_any_property = with_any_property
        self._without_any_property = without_any_property
        self._suggestion_generator = suggestion_generator

    @override
    def visit_measure_node(self, node: MeasureGroupByItemSourceNode) -> PushDownResult:
        """Push the group-by-item specs that are available to the measure and match the source patterns to the child."""
        with self._path_from_start_node_tracker.track_node_visit(node) as current_traversal_path:
            logger.info(f"Handling {node.ui_description}")
            items_available_for_measure = self._semantic_manifest_lookup.metric_lookup.linkable_elements_for_measure(
                measure_reference=node.measure_reference,
                with_any_of=self._with_any_property,
                without_any_of=self._without_any_property,
            )

            # The following is needed to handle limitation of cumulative metrics. Filtering could be done at the measure
            # node, but doing it here makes it a little easier to generate the error message.
            metric = self._semantic_manifest_lookup.metric_lookup.get_metric(node.child_metric_reference)

            patterns_to_apply: Tuple[SpecPattern, ...] = ()
            if metric.type is MetricType.SIMPLE or metric.type is MetricType.CONVERSION:
                pass
            elif metric.type is MetricType.RATIO or metric.type is MetricType.DERIVED:
                assert False, f"A measure should have a simple or cumulative metric as a child, but got {metric.type}"
            elif metric.type is MetricType.CUMULATIVE:
                # To handle the restriction that cumulative metrics can only be queried at the base grain, it's
                # easiest to handle that by applying the pattern to remove non-base grain time dimension specs at the
                # measure node and generate the issue here if there's nothing that matches. Generating the issue here
                # allows for creation of a more specific issue (i.e. include the measure) vs. generating the issue
                # at a higher level. This can be more cleanly handled once we add additional context to the
                # LinkableInstanceSpec.
                patterns_to_apply = (
                    # From comment in ValidLinkableSpecResolver:
                    #   It's possible to aggregate measures to coarser time granularities
                    #   (except with cumulative metrics).
                    BaseTimeGrainPattern(only_apply_for_metric_time=True),
                    # From comment in previous query parser:
                    #   Cannot extract date part for cumulative metrics.
                    NoneDatePartPattern(),
                )
            else:
                assert_values_exhausted(metric.type)

            matching_items = items_available_for_measure.filter_by_spec_patterns(
                patterns_to_apply + self._source_spec_patterns
            )

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"For {node.ui_description}:\n"
                    + indent(
                        "After applying patterns:\n"
                        + indent(mf_pformat(patterns_to_apply))
                        + "\n"
                        + "to inputs, matches are:\n"
                        + indent(mf_pformat(matching_items.specs))
                    )
                )

            # The specified patterns don't match to any of the available group-by-items that can be queried for the
            # measure.
            if matching_items.spec_count == 0:
                items_available_for_measure_given_child_metric = items_available_for_measure.filter_by_spec_patterns(
                    patterns_to_apply
                )
                return PushDownResult(
                    candidate_set=GroupByItemCandidateSet.empty_instance(),
                    issue_set=MetricFlowQueryResolutionIssueSet.from_issue(
                        NoMatchingItemsForMeasure.from_parameters(
                            parent_issues=(),
                            query_resolution_path=current_traversal_path,
                            input_suggestions=(
                                tuple(
                                    self._suggestion_generator.input_suggestions(
                                        items_available_for_measure_given_child_metric.specs
                                    )
                                )
                                if self._suggestion_generator is not None
                                else ()
                            ),
                        )
                    ),
                )

            return PushDownResult(
                candidate_set=GroupByItemCandidateSet(
                    measure_paths=(current_traversal_path,),
                    linkable_element_set=matching_items,
                    path_from_leaf_node=current_traversal_path,
                ),
                issue_set=MetricFlowQueryResolutionIssueSet(),
            )

    def _merge_push_down_results_from_parents(
        self,
        push_down_results_from_parents: Dict[GroupByItemResolutionNode, PushDownResult],
        current_traversal_path: MetricFlowQueryResolutionPath,
    ) -> PushDownResult:
        """Helper method that merges push down results from parents into a single result.

        This logic is shared between several nodes. In general, a node intersects the candidate sets from the parents
        to form a new candidate set, and passes that down to the child node. This handles generation of an issue if
        the parent candidate sets don't have a common candidate.
        """
        merged_issue_set: MetricFlowQueryResolutionIssueSet = MetricFlowQueryResolutionIssueSet.merge_iterable(
            parent_candidate_set.issue_set for parent_candidate_set in push_down_results_from_parents.values()
        )
        if len(push_down_results_from_parents) == 0:
            # If there are no results of any kind from the parents it means there are no parents, which means
            # something is likely wrong with the semantic manifest or the query construction itself
            merged_issue_set = merged_issue_set.add_issue(
                NoParentCandidates.from_parameters(query_resolution_path=current_traversal_path)
            )

        if merged_issue_set.has_errors:
            return PushDownResult(
                candidate_set=GroupByItemCandidateSet.empty_instance(),
                issue_set=merged_issue_set,
            )

        parent_candidate_sets = tuple(
            parent_candidate_set.candidate_set for parent_candidate_set in push_down_results_from_parents.values()
        )
        intersected_candidate_set = GroupByItemCandidateSet.intersection(
            path_from_leaf_node=current_traversal_path, candidate_sets=parent_candidate_sets
        )

        if intersected_candidate_set.is_empty:
            return PushDownResult(
                candidate_set=intersected_candidate_set,
                issue_set=merged_issue_set.add_issue(
                    NoCommonItemsInParents.from_parameters(
                        query_resolution_path=current_traversal_path,
                        parent_node_to_candidate_set={
                            parent_node: push_down_result.candidate_set
                            for parent_node, push_down_result in push_down_results_from_parents.items()
                        },
                        parent_issues=(),
                    )
                ),
            )
        return PushDownResult(
            candidate_set=intersected_candidate_set,
            issue_set=merged_issue_set,
        )

    @override
    def visit_metric_node(self, node: MetricGroupByItemResolutionNode) -> PushDownResult:
        """At the metric node, intersect candidates from the parents and pass them to the children.

        This node can represent a metric that prevents querying by some types of group-by items. To model that
        restriction, this filters the appropriate candidates.
        """
        with self._path_from_start_node_tracker.track_node_visit(node) as current_traversal_path:
            merged_result_from_parents = self._merge_push_down_results_from_parents(
                push_down_results_from_parents={
                    parent_node: parent_node.accept(self) for parent_node in node.parent_nodes
                },
                current_traversal_path=current_traversal_path,
            )
            logger.info(f"Handling {node.ui_description}")
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Candidates from parents:\n" + indent(mf_pformat(merged_result_from_parents.candidate_set.specs))
                )
            if merged_result_from_parents.candidate_set.is_empty:
                return merged_result_from_parents

            metric = self._semantic_manifest_lookup.metric_lookup.get_metric(node.metric_reference)

            # For metrics with offset_to_grain, don't allow date_part group-by-items
            patterns_to_apply: Sequence[SpecPattern] = ()
            if (
                metric.type is MetricType.SIMPLE
                or metric.type is MetricType.CUMULATIVE
                or metric.type is MetricType.CONVERSION
            ):
                pass
            elif metric.type is MetricType.RATIO or metric.type is MetricType.DERIVED:
                for input_metric in metric.input_metrics:
                    if input_metric.offset_to_grain:
                        # From comment in previous query parser:
                        # "Cannot extract date part for metrics with offset_to_grain."
                        patterns_to_apply = (NoneDatePartPattern(),)
                        break
            else:
                assert_values_exhausted(metric.type)

            candidate_items = merged_result_from_parents.candidate_set.linkable_element_set
            issue_sets_to_merge = [merged_result_from_parents.issue_set]

            matched_items = candidate_items.filter_by_spec_patterns(patterns_to_apply)

            if logger.isEnabledFor(logging.DEBUG):
                matched_specs = matched_items.specs
                logger.debug(
                    f"For {node.ui_description}:\n"
                    + indent(
                        "After applying patterns:\n"
                        + indent(mf_pformat(patterns_to_apply))
                        + "\n"
                        + "to inputs, outputs are:\n"
                        + indent(mf_pformat(matched_specs))
                    )
                )

            # There were candidates that were common from the ones passed from parents, but after applying the filters,
            # none of the candidates were valid.
            if matched_items.spec_count == 0:
                issue_sets_to_merge.append(
                    MetricFlowQueryResolutionIssueSet.from_issue(
                        MetricExcludesDatePartIssue.from_parameters(
                            query_resolution_path=current_traversal_path,
                            candidate_specs=candidate_items.specs,
                            parent_issues=(),
                        )
                    )
                )

            if matched_items.spec_count == 0:
                return PushDownResult(
                    candidate_set=GroupByItemCandidateSet.empty_instance(),
                    issue_set=MetricFlowQueryResolutionIssueSet.merge_iterable(issue_sets_to_merge),
                )

            return PushDownResult(
                candidate_set=GroupByItemCandidateSet(
                    linkable_element_set=matched_items,
                    measure_paths=merged_result_from_parents.candidate_set.measure_paths,
                    path_from_leaf_node=current_traversal_path,
                ),
                issue_set=MetricFlowQueryResolutionIssueSet.merge_iterable(issue_sets_to_merge),
            )

    @override
    def visit_query_node(self, node: QueryGroupByItemResolutionNode) -> PushDownResult:
        """Intersect candidates from the parent node and then return the result.

        The query node is a leaf node in the DAG, so the result from here is handled outside of this visitor.
        """
        with self._path_from_start_node_tracker.track_node_visit(node) as current_traversal_path:
            merged_result_from_parents = self._merge_push_down_results_from_parents(
                push_down_results_from_parents={
                    parent_node: parent_node.accept(self) for parent_node in node.parent_nodes
                },
                current_traversal_path=current_traversal_path,
            )
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Candidates from parents:\n" + indent(mf_pformat(merged_result_from_parents.candidate_set.specs))
                )

            return merged_result_from_parents

    @override
    def visit_no_metrics_query_node(self, node: NoMetricsGroupByItemSourceNode) -> PushDownResult:
        """Pass the group-by items that can be queried without metrics to the child node."""
        with self._path_from_start_node_tracker.track_node_visit(node) as current_traversal_path:
            logger.info(f"Handling {node.ui_description}")
            # This is a case for distinct dimension values from semantic models.
            candidate_elements = self._semantic_manifest_lookup.metric_lookup.linkable_elements_for_no_metrics_query()
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Candidate elements are:\n{mf_pformat(candidate_elements)}")
            candidates_after_filtering = candidate_elements.filter_by_spec_patterns(self._source_spec_patterns)

            if candidates_after_filtering.spec_count == 0:
                return PushDownResult(
                    candidate_set=GroupByItemCandidateSet.empty_instance(),
                    issue_set=MetricFlowQueryResolutionIssueSet.from_issue(
                        NoMatchingItemsForNoMetricsQuery.from_parameters(
                            parent_issues=(),
                            query_resolution_path=current_traversal_path,
                        )
                    ),
                )

            return PushDownResult(
                candidate_set=GroupByItemCandidateSet(
                    linkable_element_set=candidates_after_filtering,
                    measure_paths=(current_traversal_path,),
                    path_from_leaf_node=current_traversal_path,
                ),
                issue_set=MetricFlowQueryResolutionIssueSet.empty_instance(),
            )
