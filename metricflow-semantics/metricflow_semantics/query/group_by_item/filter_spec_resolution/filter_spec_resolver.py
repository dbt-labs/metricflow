from __future__ import annotations

import itertools
import logging
from collections import defaultdict
from typing import Dict, List, Optional, Sequence, Set, Union

from dbt_semantic_interfaces.call_parameter_sets import JinjaCallParameterSets, MetricCallParameterSet
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilterIntersection
from dbt_semantic_interfaces.protocols import WhereFilter
from dbt_semantic_interfaces.references import EntityReference
from typing_extensions import override

from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.naming.object_builder_str import ObjectBuilderNameConverter
from metricflow_semantics.query.group_by_item.candidate_push_down.push_down_visitor import DagTraversalPathTracker
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_location import (
    WhereFilterLocation,
)
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_pattern_factory import (
    WhereFilterPatternFactory,
)
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_spec_lookup import (
    FilterSpecResolution,
    FilterSpecResolutionLookUp,
    NonParsableFilterResolution,
    PatternAssociationForWhereFilterGroupByItem,
    ResolvedSpecLookUpKey,
)
from metricflow_semantics.query.group_by_item.group_by_item_resolver import GroupByItemResolver
from metricflow_semantics.query.group_by_item.resolution_dag.dag import GroupByItemResolutionDag
from metricflow_semantics.query.group_by_item.resolution_dag.input_metric_location import InputMetricDefinitionLocation
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
from metricflow_semantics.query.issues.filter_spec_resolver.invalid_where import WhereFilterParsingIssue
from metricflow_semantics.query.issues.issues_base import (
    MetricFlowQueryResolutionIssueSet,
)
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_logging.runtime import log_runtime

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

    def __init__(  # noqa: D107
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

    def __init__(
        self, manifest_lookup: SemanticManifestLookup, spec_pattern_factory: WhereFilterPatternFactory
    ) -> None:
        self._manifest_lookup = manifest_lookup
        self._path_from_start_node_tracker = DagTraversalPathTracker()
        self._spec_pattern_factory = spec_pattern_factory

    @staticmethod
    def _dedupe_filter_call_parameter_sets(
        filter_call_parameter_sets_sequence: Sequence[JinjaCallParameterSets],
    ) -> JinjaCallParameterSets:
        return JinjaCallParameterSets(
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
            metric_call_parameter_sets=tuple(
                dict.fromkeys(
                    itertools.chain.from_iterable(
                        filter_call_parameter_sets.metric_call_parameter_sets
                        for filter_call_parameter_sets in filter_call_parameter_sets_sequence
                    )
                )
            ),
        )

    def _map_filter_parameter_sets_to_pattern(
        self,
        filter_call_parameter_sets: JinjaCallParameterSets,
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
        for metric_call_parameter_set in filter_call_parameter_sets.metric_call_parameter_sets:
            # Convert LinkableElementReferences to EntityReferences. Will need to support dimensions later.
            updated_metric_call_parameter_set = MetricCallParameterSet(
                metric_reference=metric_call_parameter_set.metric_reference,
                group_by=tuple(
                    EntityReference(element_name=group_by_ref.element_name)
                    for group_by_ref in metric_call_parameter_set.group_by
                ),
            )
            patterns_in_filter.append(
                PatternAssociationForWhereFilterGroupByItem(
                    call_parameter_set=updated_metric_call_parameter_set,
                    object_builder_str=ObjectBuilderNameConverter.input_str_from_metric_call_parameter_set(
                        metric_call_parameter_set
                    ),
                    spec_pattern=self._spec_pattern_factory.create_for_metric_call_parameter_set(
                        metric_call_parameter_set
                    ),
                )
            )

        return patterns_in_filter

    def _handle_metric_node(
        self, node: Union[SimpleMetricGroupByItemSourceNode, ComplexMetricGroupByItemResolutionNode]
    ) -> FilterSpecResolutionLookUp:
        with self._path_from_start_node_tracker.track_node_visit(node) as resolution_path:
            results_to_merge: List[FilterSpecResolutionLookUp] = []
            for parent_node in node.parent_nodes:
                results_to_merge.append(parent_node.accept(self))

            where_filters_and_locations = self._get_where_filters_at_metric_node(
                metric_node=node,
                metric_input_location=node.metric_input_location,
            )

            logger.debug(
                LazyFormat(
                    "Visiting node", node=node.ui_description, where_filters_and_locations=where_filters_and_locations
                )
            )
            resolved_spec_lookup_so_far = FilterSpecResolutionLookUp.merge_iterable(results_to_merge)

            output = resolved_spec_lookup_so_far.merge(
                self._resolve_specs_for_where_filters(
                    current_node=node,
                    resolution_path=resolution_path,
                    where_filters_and_locations=where_filters_and_locations,
                )
            )

            logger.debug(LazyFormat("Generated output", node=node.ui_description, output=output))
            return output

    @override
    def visit_simple_metric_node(self, node: SimpleMetricGroupByItemSourceNode) -> FilterSpecResolutionLookUp:
        return self._handle_metric_node(node)

    @override
    def visit_complex_metric_node(self, node: ComplexMetricGroupByItemResolutionNode) -> FilterSpecResolutionLookUp:
        return self._handle_metric_node(node)

    @override
    def visit_query_node(self, node: QueryGroupByItemResolutionNode) -> FilterSpecResolutionLookUp:
        """Resolve specs for filters defined for the query."""
        with self._path_from_start_node_tracker.track_node_visit(node) as resolution_path:
            results_to_merge: List[FilterSpecResolutionLookUp] = []
            for parent_node in node.parent_nodes:
                results_to_merge.append(parent_node.accept(self))

            # If the same metric is present multiple times in a query - there could be duplicates.
            resolved_spec_lookup_so_far = FilterSpecResolutionLookUp.merge_iterable(results_to_merge)

            logger.debug(LazyFormat("Visiting node", node=node.ui_description))
            output = resolved_spec_lookup_so_far.merge(
                self._resolve_specs_for_where_filters(
                    current_node=node,
                    resolution_path=resolution_path,
                    where_filters_and_locations={
                        WhereFilterLocation.for_query(node.metrics_in_query): set(
                            node.where_filter_intersection.where_filters
                        )
                    },
                )
            )

            logger.debug(LazyFormat("Generated output", node=node.ui_description, output=output))
            return output

    @override
    def visit_no_metrics_query_node(self, node: NoMetricsGroupByItemSourceNode) -> FilterSpecResolutionLookUp:
        """Similar to the simple-metric input node - filters are applied at the query level."""
        with self._path_from_start_node_tracker.track_node_visit(node):
            logger.debug(LazyFormat("Visiting node", node=node.ui_description))
            output = FilterSpecResolutionLookUp.empty_instance()
            logger.debug(LazyFormat("Generated output", node=node.ui_description, output=output))
            return output

    def _get_where_filters_at_metric_node(
        self,
        metric_node: Union[SimpleMetricGroupByItemSourceNode, ComplexMetricGroupByItemResolutionNode],
        metric_input_location: Optional[InputMetricDefinitionLocation],
    ) -> Dict[WhereFilterLocation, Set[WhereFilter]]:
        """Return the filters used with a metric in the manifest.

        A derived metric definition can have a filter for an input metric, and we'll need to resolve that when the
        dataflow plan is built.
        """
        where_filters_and_locations: Dict[WhereFilterLocation, Set[WhereFilter]] = defaultdict(set)
        metric = self._manifest_lookup.metric_lookup.get_metric(metric_node.metric_reference)

        metric_filter_location = WhereFilterLocation.for_metric(metric_reference=metric_node.metric_reference)
        if metric.filter is not None:
            for metric_where_filter in metric.filter.where_filters:
                where_filters_and_locations[metric_filter_location].add(metric_where_filter)

        # This is a metric that is an input metric for a derived metric. The derived metric is the child node of this
        # metric node.
        if metric_input_location is not None:
            child_metric_input = metric_input_location.get_metric_input(self._manifest_lookup.metric_lookup)
            if child_metric_input.filter is not None:
                input_metric_filter_location = WhereFilterLocation.for_input_metric(
                    input_metric_reference=metric_node.metric_reference
                )
                for input_metric_where_filter in child_metric_input.filter.where_filters:
                    where_filters_and_locations[input_metric_filter_location].add(input_metric_where_filter)

        return where_filters_and_locations

    def _resolve_specs_for_where_filters(
        self,
        current_node: GroupByItemResolutionNode,
        resolution_path: MetricFlowQueryResolutionPath,
        where_filters_and_locations: Dict[WhereFilterLocation, Set[WhereFilter]],
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
        filter_call_parameter_sets_by_location: Dict[WhereFilterLocation, List[JinjaCallParameterSets]] = defaultdict(
            list
        )
        # No input metric in locations when we get here
        for location, where_filters in where_filters_and_locations.items():
            for where_filter in where_filters:
                try:
                    filter_call_parameter_sets = where_filter.call_parameter_sets(
                        custom_granularity_names=self._manifest_lookup.semantic_model_lookup.custom_granularity_names
                    )
                except Exception as e:
                    non_parsable_resolutions.append(
                        NonParsableFilterResolution(
                            filter_location_path=resolution_path,
                            where_filter_intersection=PydanticWhereFilterIntersection(
                                where_filters=list(
                                    {
                                        where_filter
                                        for where_filter_set in where_filters_and_locations.values()
                                        for where_filter in where_filter_set
                                    }
                                )
                            ),
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
                filter_call_parameter_sets_by_location[location].append(filter_call_parameter_sets)

        deduped_filter_call_parameter_sets_by_location = {
            location: _ResolveWhereFilterSpecVisitor._dedupe_filter_call_parameter_sets(filter_call_parameter_sets_list)
            for location, filter_call_parameter_sets_list in filter_call_parameter_sets_by_location.items()
        }

        resolutions: List[FilterSpecResolution] = []
        for (
            filter_location,
            deduped_filter_call_parameter_sets,
        ) in deduped_filter_call_parameter_sets_by_location.items():
            for group_by_item_in_where_filter in self._map_filter_parameter_sets_to_pattern(
                filter_call_parameter_sets=deduped_filter_call_parameter_sets,
            ):
                group_by_item_resolution = group_by_item_resolver.resolve_matching_item_for_filters(
                    input_str=group_by_item_in_where_filter.object_builder_str,
                    spec_pattern=group_by_item_in_where_filter.spec_pattern,
                    resolution_node=current_node,
                    filter_location=filter_location,
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
                        resolved_group_by_item_set=group_by_item_resolution.linkable_element_set,
                        where_filter_intersection=PydanticWhereFilterIntersection(
                            where_filters=list(where_filters_and_locations[filter_location])
                        ),
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
