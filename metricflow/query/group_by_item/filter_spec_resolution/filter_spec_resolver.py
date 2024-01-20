from __future__ import annotations

import itertools
import logging
from typing import List, Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    FilterCallParameterSets,
)
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilterIntersection
from dbt_semantic_interfaces.protocols import WhereFilter, WhereFilterIntersection
from typing_extensions import override

from metricflow.mf_logging.runtime import log_runtime
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.naming.object_builder_str import ObjectBuilderNameConverter
from metricflow.query.group_by_item.candidate_push_down.push_down_visitor import DagTraversalPathTracker
from metricflow.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow.query.group_by_item.filter_spec_resolution.filter_pattern_factory import WhereFilterPatternFactory
from metricflow.query.group_by_item.filter_spec_resolution.filter_spec_lookup import (
    FilterSpecResolution,
    FilterSpecResolutionLookUp,
    NonParsableFilterResolution,
    PatternAssociationForWhereFilterGroupByItem,
    ResolvedSpecLookUpKey,
)
from metricflow.query.group_by_item.group_by_item_resolver import GroupByItemResolver
from metricflow.query.group_by_item.resolution_dag.dag import GroupByItemResolutionDag, ResolutionDagSinkNode
from metricflow.query.group_by_item.resolution_dag.resolution_nodes.base_node import (
    GroupByItemResolutionNodeVisitor,
)
from metricflow.query.group_by_item.resolution_dag.resolution_nodes.measure_source_node import (
    MeasureGroupByItemSourceNode,
)
from metricflow.query.group_by_item.resolution_dag.resolution_nodes.metric_resolution_node import (
    MetricGroupByItemResolutionNode,
)
from metricflow.query.group_by_item.resolution_dag.resolution_nodes.no_metrics_query_source_node import (
    NoMetricsGroupByItemSourceNode,
)
from metricflow.query.group_by_item.resolution_dag.resolution_nodes.query_resolution_node import (
    QueryGroupByItemResolutionNode,
)
from metricflow.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow.query.issues.filter_spec_resolver.invalid_where import WhereFilterParsingIssue
from metricflow.query.issues.issues_base import (
    MetricFlowQueryResolutionIssueSet,
)

logger = logging.getLogger(__name__)


class WhereFilterSpecResolver:
    """Resolves the specs for the (ambiguous) group-by-items that are in where filters of the resolution DAG.

    In the resolution DAG, filters can occur:

    * In the query.
    * In the definition of a metric as a filter.
    * In the definition of an input measure to a base metric.
    * In the definition of an input metric to a derived metric.

    The concrete specs for the group-by-items are returned in a lookup.
    """

    def __init__(  # noqa: D
        self,
        manifest_lookup: SemanticManifestLookup,
        resolution_dag: GroupByItemResolutionDag,
        spec_pattern_factory: WhereFilterPatternFactory,
    ) -> None:
        self._manifest_lookup = manifest_lookup
        self._resolution_dag = resolution_dag
        self.spec_pattern_factory = spec_pattern_factory

    def resolve_lookup(self) -> FilterSpecResolutionLookUp:
        """Find all where filters and return a lookup that provides the specs for the included group-by-items."""
        # Workaround for a Pycharm type inspection issue with decorators.
        # noinspection PyArgumentList
        return self._resolve_lookup()

    @log_runtime()
    def _resolve_lookup(self) -> FilterSpecResolutionLookUp:
        visitor = _ResolveWhereFilterSpecVisitor(
            manifest_lookup=self._manifest_lookup,
            spec_pattern_factory=self.spec_pattern_factory,
        )
        return self._resolution_dag.sink_node.accept(visitor)


class _ResolveWhereFilterSpecVisitor(GroupByItemResolutionNodeVisitor[FilterSpecResolutionLookUp]):
    """Visitor that resolves specs for all filters in a resolution DAG and builds a lookup.

    This visitor traverses the group-by-item resolution DAG and if there is a where filter associated with the node,
    this resolves (ambiguous) group-by-items that are specified in the filter. All resolutions are
    collected and returned in a lookup object.
    """

    def __init__(  # noqa: D
        self, manifest_lookup: SemanticManifestLookup, spec_pattern_factory: WhereFilterPatternFactory
    ) -> None:
        self._manifest_lookup = manifest_lookup
        self._path_from_start_node_tracker = DagTraversalPathTracker()
        self._spec_pattern_factory = spec_pattern_factory

    @staticmethod
    def _dedupe_filter_call_parameter_sets(
        filter_call_parameter_sets_sequence: Sequence[FilterCallParameterSets],
    ) -> FilterCallParameterSets:
        # FilterCallParameterSets needs an update.
        return FilterCallParameterSets(
            dimension_call_parameter_sets=tuple(
                dict.fromkeys(
                    itertools.chain.from_iterable(
                        filter_call_parameter_sets.dimension_call_parameter_sets
                        for filter_call_parameter_sets in filter_call_parameter_sets_sequence
                    )
                )
            ),
            time_dimension_call_parameter_sets=tuple(
                dict.fromkeys(
                    itertools.chain.from_iterable(
                        filter_call_parameter_sets.time_dimension_call_parameter_sets
                        for filter_call_parameter_sets in filter_call_parameter_sets_sequence
                    )
                )
            ),
            entity_call_parameter_sets=tuple(
                dict.fromkeys(
                    itertools.chain.from_iterable(
                        filter_call_parameter_sets.entity_call_parameter_sets
                        for filter_call_parameter_sets in filter_call_parameter_sets_sequence
                    )
                )
            ),
        )

    def _map_filter_parameter_sets_to_pattern(
        self,
        filter_call_parameter_sets: FilterCallParameterSets,
    ) -> Sequence[PatternAssociationForWhereFilterGroupByItem]:
        """Given the call parameter sets in a filter, map them to spec patterns.

        If a given call parameter set has already been resolved and in spec_resolution_lookup_so_far, it is skipped and
        not returned in the dictionary.

        This assumes that the items in filter_call_parameter_sets have been deduped.
        """
        patterns_in_filter: List[PatternAssociationForWhereFilterGroupByItem] = []
        for dimension_call_parameter_set in filter_call_parameter_sets.dimension_call_parameter_sets:
            patterns_in_filter.append(
                PatternAssociationForWhereFilterGroupByItem(
                    call_parameter_set=dimension_call_parameter_set,
                    object_builder_str=ObjectBuilderNameConverter.input_str_from_dimension_call_parameter_set(
                        dimension_call_parameter_set
                    ),
                    spec_pattern=self._spec_pattern_factory.create_for_dimension_call_parameter_set(
                        dimension_call_parameter_set
                    ),
                )
            )
        for time_dimension_call_parameter_set in filter_call_parameter_sets.time_dimension_call_parameter_sets:
            patterns_in_filter.append(
                PatternAssociationForWhereFilterGroupByItem(
                    call_parameter_set=time_dimension_call_parameter_set,
                    object_builder_str=ObjectBuilderNameConverter.input_str_from_time_dimension_call_parameter_set(
                        time_dimension_call_parameter_set
                    ),
                    spec_pattern=self._spec_pattern_factory.create_for_time_dimension_call_parameter_set(
                        time_dimension_call_parameter_set
                    ),
                )
            )
        for entity_call_parameter_set in filter_call_parameter_sets.entity_call_parameter_sets:
            patterns_in_filter.append(
                PatternAssociationForWhereFilterGroupByItem(
                    call_parameter_set=entity_call_parameter_set,
                    object_builder_str=ObjectBuilderNameConverter.input_str_from_entity_call_parameter_set(
                        entity_call_parameter_set
                    ),
                    spec_pattern=self._spec_pattern_factory.create_for_entity_call_parameter_set(
                        entity_call_parameter_set
                    ),
                )
            )

        return patterns_in_filter

    @override
    def visit_measure_node(self, node: MeasureGroupByItemSourceNode) -> FilterSpecResolutionLookUp:
        """No filters are defined in measures, so this is a no-op."""
        with self._path_from_start_node_tracker.track_node_visit(node):
            return FilterSpecResolutionLookUp.empty_instance()

    @override
    def visit_metric_node(self, node: MetricGroupByItemResolutionNode) -> FilterSpecResolutionLookUp:
        """Resolve specs for filters in a metric definition."""
        with self._path_from_start_node_tracker.track_node_visit(node) as resolution_path:
            results_to_merge: List[FilterSpecResolutionLookUp] = []
            for parent_node in node.parent_nodes:
                results_to_merge.append(parent_node.accept(self))

            resolved_spec_lookup_so_far = FilterSpecResolutionLookUp.merge_iterable(results_to_merge)

            return resolved_spec_lookup_so_far.merge(
                self._resolve_specs_for_where_filters(
                    current_node=node,
                    resolution_path=resolution_path,
                    filter_location=WhereFilterLocation.for_metric(node.metric_reference),
                    where_filter_intersection=self._get_where_filters_at_metric_node(node),
                )
            )

    @override
    def visit_query_node(self, node: QueryGroupByItemResolutionNode) -> FilterSpecResolutionLookUp:
        """Resolve specs for filters defined for the query."""
        with self._path_from_start_node_tracker.track_node_visit(node) as resolution_path:
            results_to_merge: List[FilterSpecResolutionLookUp] = []
            for parent_node in node.parent_nodes:
                results_to_merge.append(parent_node.accept(self))

            # If the same metric is present multiple times in a query - there could be duplicates.
            resolved_spec_lookup_so_far = FilterSpecResolutionLookUp.merge_iterable(results_to_merge)

            return resolved_spec_lookup_so_far.merge(
                self._resolve_specs_for_where_filters(
                    current_node=node,
                    resolution_path=resolution_path,
                    filter_location=WhereFilterLocation.for_query(node.metrics_in_query),
                    where_filter_intersection=node.where_filter_intersection,
                )
            )

    @override
    def visit_no_metrics_query_node(self, node: NoMetricsGroupByItemSourceNode) -> FilterSpecResolutionLookUp:
        """Similar to the measure node - filters are applied at the query level."""
        with self._path_from_start_node_tracker.track_node_visit(node):
            return FilterSpecResolutionLookUp.empty_instance()

    def _get_where_filters_at_metric_node(
        self, metric_node: MetricGroupByItemResolutionNode
    ) -> WhereFilterIntersection:
        """Return the filters used with a metric in the manifest.

        A derived metric definition can have a filter for an input metric, and we'll need to resolve that when the
        dataflow plan is built.
        """
        where_filters: List[WhereFilter] = []
        metric = self._manifest_lookup.metric_lookup.get_metric(metric_node.metric_reference)

        if metric.input_measures is not None:
            for input_measure in metric.input_measures:
                if input_measure.filter is not None:
                    where_filters.extend(input_measure.filter.where_filters)

        if metric.filter is not None:
            where_filters.extend(metric.filter.where_filters)

        # This is a metric that is an input metric for a derived metric. The derived metric is the child node of this
        # metric node.
        if metric_node.metric_input_location is not None:
            child_metric_input = metric_node.metric_input_location.get_metric_input(self._manifest_lookup.metric_lookup)
            if child_metric_input.filter is not None:
                where_filters.extend(child_metric_input.filter.where_filters)

        return PydanticWhereFilterIntersection(where_filters=where_filters)

    def _resolve_specs_for_where_filters(
        self,
        current_node: ResolutionDagSinkNode,
        resolution_path: MetricFlowQueryResolutionPath,
        filter_location: WhereFilterLocation,
        where_filter_intersection: WhereFilterIntersection,
    ) -> FilterSpecResolutionLookUp:
        """Given the filters at a particular node, build the spec lookup for those filters.

        The start node should be the query node.
        """
        resolution_dag = GroupByItemResolutionDag(
            sink_node=current_node,
        )
        group_by_item_resolver = GroupByItemResolver(
            manifest_lookup=self._manifest_lookup,
            resolution_dag=resolution_dag,
        )
        non_parsable_resolutions: List[NonParsableFilterResolution] = []
        filter_call_parameter_sets_to_merge: List[FilterCallParameterSets] = []

        for where_filter in where_filter_intersection.where_filters:
            try:
                filter_call_parameter_sets = where_filter.call_parameter_sets
            except Exception as e:
                non_parsable_resolutions.append(
                    NonParsableFilterResolution(
                        filter_location_path=resolution_path,
                        where_filter_intersection=where_filter_intersection,
                        issue_set=MetricFlowQueryResolutionIssueSet.from_issue(
                            WhereFilterParsingIssue.from_parameters(
                                where_filter=where_filter,
                                parse_exception=e,
                                query_resolution_path=resolution_path,
                            )
                        ),
                    )
                )
                continue
            filter_call_parameter_sets_to_merge.append(filter_call_parameter_sets)

        deduped_filter_call_parameter_sets = _ResolveWhereFilterSpecVisitor._dedupe_filter_call_parameter_sets(
            filter_call_parameter_sets_to_merge
        )

        resolutions: List[FilterSpecResolution] = []
        for group_by_item_in_where_filter in self._map_filter_parameter_sets_to_pattern(
            filter_call_parameter_sets=deduped_filter_call_parameter_sets,
        ):
            group_by_item_resolution = group_by_item_resolver.resolve_matching_item_for_filters(
                input_str=group_by_item_in_where_filter.object_builder_str,
                spec_pattern=group_by_item_in_where_filter.spec_pattern,
                resolution_node=current_node,
            )
            # The paths in the issue set are generated relative to the current node. For error messaging, it seems more
            # helpful for those paths to be relative to the query. To do, we have to add nodes from the resolution path.
            # e.g. if the current node is B, and the resolution path is [A, B], an issue might have the relative path
            # [B, C]. To join those paths to produce [A, B, C], the path prefix should be [A].
            path_prefix = MetricFlowQueryResolutionPath(
                resolution_path_nodes=resolution_path.resolution_path_nodes[:-1]
            )
            resolutions.append(
                FilterSpecResolution(
                    lookup_key=ResolvedSpecLookUpKey(
                        filter_location=filter_location,
                        call_parameter_set=group_by_item_in_where_filter.call_parameter_set,
                    ),
                    filter_location_path=resolution_path,
                    resolved_spec=group_by_item_resolution.spec,
                    where_filter_intersection=where_filter_intersection,
                    spec_pattern=group_by_item_in_where_filter.spec_pattern,
                    issue_set=group_by_item_resolution.issue_set.with_path_prefix(path_prefix),
                    object_builder_str=group_by_item_in_where_filter.object_builder_str,
                )
            )

        if any(resolution.issue_set.has_errors for resolution in resolutions) or len(non_parsable_resolutions) > 0:
            return FilterSpecResolutionLookUp(
                spec_resolutions=tuple(resolution for resolution in resolutions if resolution.issue_set.has_errors),
                non_parsable_resolutions=tuple(non_parsable_resolutions),
            )

        return FilterSpecResolutionLookUp(
            spec_resolutions=tuple(resolutions),
            non_parsable_resolutions=tuple(non_parsable_resolutions),
        )
