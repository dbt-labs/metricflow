from __future__ import annotations

import logging
import typing
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols import Metric
from dbt_semantic_interfaces.references import MetricReference
from dbt_semantic_interfaces.type_enums import MetricType
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from typing_extensions import override

from metricflow_semantics.errors.custom_grain_not_supported import error_if_not_standard_grain
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
from metricflow_semantics.query.group_by_item.candidate_push_down.group_by_item_candidate import GroupByItemCandidateSet
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_location import (
    WhereFilterLocation,
    WhereFilterLocationType,
)
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.base_node import (
    GroupByItemResolutionNode,
    GroupByItemResolutionNodeVisitor,
)
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.metric_resolution_node import (
    ComplexMetricGroupByItemResolutionNode,
)
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.no_metrics_query_source_node import (
    NoMetricsGroupByItemSourceNode,
)
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.query_resolution_node import (
    QueryGroupByItemResolutionNode,
)
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.simple_metric_source_node import (
    SimpleMetricGroupByItemSourceNode,
)
from metricflow_semantics.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow_semantics.query.issues.group_by_item_resolver.invalid_use_of_date_part import (
    MetricExcludesDatePartIssue,
)
from metricflow_semantics.query.issues.group_by_item_resolver.no_common_items import NoCommonItemsInParents
from metricflow_semantics.query.issues.group_by_item_resolver.no_matching_items_for_no_metrics_query import (
    NoMatchingItemsForNoMetricsQuery,
)
from metricflow_semantics.query.issues.group_by_item_resolver.no_matching_items_for_simple_metric import (
    NoMatchingItemsForSimpleMetric,
)
from metricflow_semantics.query.issues.group_by_item_resolver.no_parent_candidates import NoParentCandidates
from metricflow_semantics.query.issues.issues_base import (
    MetricFlowQueryResolutionIssueSet,
)
from metricflow_semantics.query.suggestion_generator import QueryItemSuggestionGenerator
from metricflow_semantics.specs.patterns.none_date_part import NoneDatePartPattern
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_logging.pretty_print import mf_pformat, mf_pformat_dict
from metricflow_semantics.toolkit.string_helpers import mf_indent

if typing.TYPE_CHECKING:
    from metricflow_semantics.query.group_by_item.group_by_item_resolver import GroupByItemResolver


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PushDownResult:
    """The result that is pushed down from the root nodes to the leaf nodes to resolve ambiguous group-by items."""

    # The set of candidate specs that could match the ambiguous group-by item.
    candidate_set: GroupByItemCandidateSet
    # The issues seen so far while pushing down the result / resolving the ambiguity.
    issue_set: MetricFlowQueryResolutionIssueSet
    # The largest default time granularity of the metrics seen in the DAG so far. Used to resolve metric_time.
    # TODO: [custom granularity] decide whether or not to support custom granularities as metric_time defaults and
    # update accordingly
    max_metric_default_time_granularity: Optional[TimeGranularity] = None

    def __post_init__(self) -> None:  # noqa: D105
        # If there are no errors, there should be a candidate spec in each candidate set.
        # If there are errors, there shouldn't be any candidate sets.
        assert (not self.issue_set.has_errors and not self.candidate_set.is_empty) or (
            self.issue_set.has_errors and self.candidate_set.is_empty
        ), mf_pformat_dict(
            "candidate_set / issue_set mismatch:", {"candidate_set": self.candidate_set, "issue_set": self.issue_set}
        )

    def filter_candidates_by_pattern(self, spec_pattern: SpecPattern) -> PushDownResult:
        """Create a new result where only the candidates matching the pattern are included."""
        return PushDownResult(
            candidate_set=self.candidate_set.filter_candidates_by_pattern(spec_pattern),
            issue_set=self.issue_set,
            max_metric_default_time_granularity=self.max_metric_default_time_granularity,
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
            max_metric_default_time_granularity=self.max_metric_default_time_granularity,
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
    ambiguous group-by-item pattern from the root nodes (representing simple-metric inputs / the specs available for those
    simple-metric inputs) to the leaf node (the query for metrics / valid group-by items for querying). For nodes with multiple
    parents, the candidates from each parent are intersected and passed to the child node.
    """

    def __init__(
        self,
        manifest_lookup: SemanticManifestLookup,
        suggestion_generator: Optional[QueryItemSuggestionGenerator],
        group_by_item_resolver: GroupByItemResolver,
        source_spec_patterns: Sequence[SpecPattern] = (),
        filter_location: Optional[WhereFilterLocation] = None,
    ) -> None:
        """Initializer.

        Args:
            manifest_lookup: The semantic manifest lookup associated with the resolution DAG that this will traverse.
            group_by_item_resolver: The group-by-item resolver for the query to help generate suggestions.
            suggestion_generator: If there are issues with matching patterns to specs, use this to generate suggestions
            that will go in the issue.
            source_spec_patterns: The patterns to apply to the specs available at the measure nodes.
            GroupByItemProperty).
            filter_location: If resolving a where filter item, where this filter was defined.
        """
        self._semantic_manifest_lookup = manifest_lookup
        self._source_spec_patterns = tuple(source_spec_patterns)
        self._path_from_start_node_tracker = DagTraversalPathTracker()
        self._suggestion_generator = suggestion_generator
        self._filter_location = filter_location
        self._group_by_item_resolver_for_query = group_by_item_resolver

    @override
    def visit_simple_metric_node(self, node: SimpleMetricGroupByItemSourceNode) -> PushDownResult:
        """Push the group-by-item specs that are available to the simple-metric input and match the source patterns to the child."""
        with self._path_from_start_node_tracker.track_node_visit(node) as current_traversal_path:
            logger.debug(LazyFormat("Visiting Node", node=node.ui_description))

            group_by_item_set = self._semantic_manifest_lookup.metric_lookup.get_common_group_by_items(
                metric_references=(node.metric_reference,),
                # The filter should allow everything, except for the ones blocked by the spec patterns.
                set_filter=GroupByItemSetFilter.create().merge(
                    GroupByItemSetFilter.merge_iterable(
                        spec_pattern.element_pre_filter for spec_pattern in self._source_spec_patterns
                    ),
                ),
            )
            patterns_to_apply = self._source_spec_patterns

            matching_items = group_by_item_set.filter_by_spec_patterns(patterns_to_apply)
            logger.debug(
                LazyFormat(
                    "Output after applying patterns", patterns_to_apply=patterns_to_apply, matching_items=matching_items
                )
            )

            metric_to_use_for_time_granularity_resolution = self._semantic_manifest_lookup.metric_lookup.get_metric(
                node.metric_reference
            )
            # If this is resolving a filter defined on an input metric, use the outer metric's time_granularity.
            if (
                node.metric_input_location
                and self._filter_location
                and self._filter_location.location_type == WhereFilterLocationType.INPUT_METRIC
            ):
                metric_to_use_for_time_granularity_resolution = self._semantic_manifest_lookup.metric_lookup.get_metric(
                    node.metric_input_location.derived_metric_reference
                )

            metric_default_time_granularity = self._get_metric_default_time_grain(
                metric_to_use_for_time_granularity_resolution
            )

            if matching_items.is_empty:
                input_suggestions: Sequence[str] = ()
                if self._suggestion_generator is not None:
                    candidate_specs = self._group_by_item_resolver_for_query.resolve_available_items(
                        source_spec_patterns=self._suggestion_generator.candidate_filters
                    ).specs
                    input_suggestions = self._suggestion_generator.input_suggestions(tuple(candidate_specs))

                return PushDownResult(
                    candidate_set=GroupByItemCandidateSet.empty_instance(),
                    issue_set=MetricFlowQueryResolutionIssueSet.from_issue(
                        NoMatchingItemsForSimpleMetric.create(
                            parent_issues=(),
                            query_resolution_path=current_traversal_path,
                            input_suggestions=input_suggestions,
                        )
                    ),
                )

            return PushDownResult(
                candidate_set=GroupByItemCandidateSet(
                    simple_metric_input_paths=(current_traversal_path,),
                    linkable_element_set=matching_items,
                    path_from_leaf_node=current_traversal_path,
                ),
                issue_set=MetricFlowQueryResolutionIssueSet(),
                max_metric_default_time_granularity=metric_default_time_granularity,
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

        metric_default_time_granularities = {
            parent_candidate_set.max_metric_default_time_granularity
            for parent_candidate_set in push_down_results_from_parents.values()
            if parent_candidate_set.max_metric_default_time_granularity
        }
        max_metric_default_time_granularity = (
            max(metric_default_time_granularities) if metric_default_time_granularities else None
        )
        if merged_issue_set.has_errors:
            return PushDownResult(
                candidate_set=GroupByItemCandidateSet.empty_instance(),
                issue_set=merged_issue_set,
                max_metric_default_time_granularity=max_metric_default_time_granularity,
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
                max_metric_default_time_granularity=max_metric_default_time_granularity,
            )
        return PushDownResult(
            candidate_set=intersected_candidate_set,
            issue_set=merged_issue_set,
            max_metric_default_time_granularity=max_metric_default_time_granularity,
        )

    def _get_metric_default_time_grain(self, metric: Metric) -> TimeGranularity:
        # If time granularity is not set for the metric, defaults to DAY if available, else the smallest available granularity.
        # Note: ignores any granularity set on input metrics.
        metric_time_granularity: Optional[TimeGranularity] = None
        if metric.time_granularity is not None:
            metric_time_granularity = error_if_not_standard_grain(
                context=f"Metric({metric.name}).time_granularity",
                input_granularity=metric.time_granularity,
            )
        return metric_time_granularity or max(
            TimeGranularity.DAY,
            self._semantic_manifest_lookup.metric_lookup.get_min_queryable_time_granularity(
                MetricReference(metric.name)
            ),
        )

    @override
    def visit_complex_metric_node(self, node: ComplexMetricGroupByItemResolutionNode) -> PushDownResult:
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
            logger.debug(
                LazyFormat("Visiting Node", node=node.ui_description, input=merged_result_from_parents.candidate_set)
            )
            if merged_result_from_parents.candidate_set.is_empty:
                return merged_result_from_parents

            metric = self._semantic_manifest_lookup.metric_lookup.get_metric(node.metric_reference)

            # For metrics with offset_to_grain, don't allow date_part group-by-items
            patterns_to_apply: Sequence[SpecPattern] = ()
            if metric.type is MetricType.SIMPLE:
                pass
            elif metric.type is MetricType.CONVERSION:
                pass
            elif metric.type is MetricType.CUMULATIVE:
                patterns_to_apply = (NoneDatePartPattern(),)
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

            matching_items = candidate_items.filter_by_spec_patterns(patterns_to_apply)

            logger.debug(
                LazyFormat(
                    "Output after applying patterns",
                    node=node.ui_description,
                    patterns_to_apply=patterns_to_apply,
                    matching_items=matching_items,
                )
            )

            # There were candidates that were common from the ones passed from parents, but after applying the filters,
            # none of the candidates were valid.
            if matching_items.is_empty:
                issue_sets_to_merge.append(
                    MetricFlowQueryResolutionIssueSet.from_issue(
                        MetricExcludesDatePartIssue.from_parameters(
                            query_resolution_path=current_traversal_path,
                            candidate_specs=candidate_items.specs,
                            parent_issues=(),
                        )
                    )
                )

            metric_to_use_for_time_granularity_resolution = metric
            # If this is resolving a filter defined on an input metric, use the outer metric's time_granularity.
            if (
                node.metric_input_location
                and self._filter_location
                and self._filter_location.location_type == WhereFilterLocationType.INPUT_METRIC
            ):
                metric_to_use_for_time_granularity_resolution = self._semantic_manifest_lookup.metric_lookup.get_metric(
                    node.metric_input_location.derived_metric_reference
                )

            metric_default_time_granularity = self._get_metric_default_time_grain(
                metric_to_use_for_time_granularity_resolution
            )

            if matching_items.is_empty:
                return PushDownResult(
                    candidate_set=GroupByItemCandidateSet.empty_instance(),
                    issue_set=MetricFlowQueryResolutionIssueSet.merge_iterable(issue_sets_to_merge),
                    max_metric_default_time_granularity=metric_default_time_granularity,
                )

            return PushDownResult(
                candidate_set=GroupByItemCandidateSet(
                    linkable_element_set=matching_items,
                    simple_metric_input_paths=merged_result_from_parents.candidate_set.simple_metric_input_paths,
                    path_from_leaf_node=current_traversal_path,
                ),
                issue_set=MetricFlowQueryResolutionIssueSet.merge_iterable(issue_sets_to_merge),
                max_metric_default_time_granularity=metric_default_time_granularity,
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
                    LazyFormat(
                        lambda: "Candidates from parents:\n"
                        + mf_indent(mf_pformat(merged_result_from_parents.candidate_set.specs))
                    )
                )

            return merged_result_from_parents

    @override
    def visit_no_metrics_query_node(self, node: NoMetricsGroupByItemSourceNode) -> PushDownResult:
        """Pass the group-by items that can be queried without metrics to the child node."""
        with self._path_from_start_node_tracker.track_node_visit(node) as current_traversal_path:
            logger.debug(LazyFormat(lambda: f"Handling {node.ui_description}"))
            # This is a case for distinct dimension values from semantic models.
            candidate_group_by_items = (
                self._semantic_manifest_lookup.metric_lookup.get_group_by_items_for_distinct_values_query()
            )
            logger.debug(LazyFormat("Retrieved candidates", candidate_group_by_items=candidate_group_by_items))
            candidates_after_filtering = candidate_group_by_items.filter_by_spec_patterns(self._source_spec_patterns)

            if candidates_after_filtering.is_empty:
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
                    simple_metric_input_paths=(current_traversal_path,),
                    path_from_leaf_node=current_traversal_path,
                ),
                issue_set=MetricFlowQueryResolutionIssueSet.empty_instance(),
            )
