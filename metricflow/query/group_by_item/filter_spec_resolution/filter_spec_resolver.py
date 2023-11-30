from __future__ import annotations

import logging
from typing import Dict, List, Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    FilterCallParameterSets,
)
from dbt_semantic_interfaces.protocols import WhereFilter
from typing_extensions import override

from metricflow.collection_helpers.pretty_print import mf_pformat
from metricflow.formatting import indent_log_line
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.query.group_by_item.candidate_push_down.push_down_visitor import DagTraversalPathTracker
from metricflow.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow.query.group_by_item.filter_spec_resolution.filter_spec_lookup import (
    CallParameterSet,
    FilterSpecResolution,
    FilterSpecResolutionLookUp,
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
from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.patterns.typed_patterns import DimensionPattern, EntityPattern, TimeDimensionPattern

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
    ) -> None:
        self._manifest_lookup = manifest_lookup
        self._resolution_dag = resolution_dag

    def resolve_lookup(self) -> FilterSpecResolutionLookUp:
        """Find all where filters and return a lookup that provides the specs for the included group-by-items."""
        visitor = _ResolveWhereFilterSpecVisitor(manifest_lookup=self._manifest_lookup)

        return self._resolution_dag.sink_node.accept(visitor)


class _ResolveWhereFilterSpecVisitor(GroupByItemResolutionNodeVisitor[FilterSpecResolutionLookUp]):
    """Visitor that resolves specs for all filters in a resolution DAG and builds a lookup.

    This visitor traverses the group-by-item resolution DAG and if there is a where filter associated with the node,
    this resolves (ambiguous) group-by-items that are specified in the filter. All resolutions are
    collected and returned in a lookup object.
    """

    def __init__(self, manifest_lookup: SemanticManifestLookup) -> None:  # noqa: D
        self._manifest_lookup = manifest_lookup
        self._path_from_start_node_tracker = DagTraversalPathTracker()

    @staticmethod
    def _map_filter_parameter_set_to_pattern(
        filter_location: WhereFilterLocation,
        filter_call_parameter_sets: FilterCallParameterSets,
        spec_resolution_lookup_so_far: FilterSpecResolutionLookUp,
    ) -> Dict[CallParameterSet, SpecPattern]:
        """Given the call parameter sets in a filter, map them to spec patterns.

        If a given call parameter set has already been resolved and in spec_resolution_lookup_so_far, it is skipped and
        not returned in the dictionary.
        """
        call_parameter_set_to_spec_pattern: Dict[CallParameterSet, SpecPattern] = {}
        for dimension_call_parameter_set in filter_call_parameter_sets.dimension_call_parameter_sets:
            lookup_key = ResolvedSpecLookUpKey(
                filter_location=filter_location,
                call_parameter_set=dimension_call_parameter_set,
            )
            if spec_resolution_lookup_so_far.spec_resolution_exists(lookup_key):
                logger.info(
                    f"Skipping resolution for {dimension_call_parameter_set} at {filter_location} since it has already"
                    f"been resolved to:\n\n"
                    f"{indent_log_line(mf_pformat(spec_resolution_lookup_so_far.get_spec_resolutions(lookup_key)))}"
                )
                continue
            if dimension_call_parameter_set not in call_parameter_set_to_spec_pattern:
                call_parameter_set_to_spec_pattern[
                    dimension_call_parameter_set
                ] = DimensionPattern.from_call_parameter_set(dimension_call_parameter_set)
        for time_dimension_call_parameter_set in filter_call_parameter_sets.time_dimension_call_parameter_sets:
            lookup_key = ResolvedSpecLookUpKey(
                filter_location=filter_location,
                call_parameter_set=time_dimension_call_parameter_set,
            )
            if spec_resolution_lookup_so_far.spec_resolution_exists(lookup_key):
                logger.info(
                    f"Skipping resolution for {time_dimension_call_parameter_set} at {filter_location} since it has "
                    f"been resolved to:\n"
                    f"{mf_pformat(spec_resolution_lookup_so_far.get_spec_resolutions(lookup_key))}"
                )
            if time_dimension_call_parameter_set not in call_parameter_set_to_spec_pattern:
                call_parameter_set_to_spec_pattern[
                    time_dimension_call_parameter_set
                ] = TimeDimensionPattern.from_call_parameter_set(time_dimension_call_parameter_set)
        for entity_call_parameter_set in filter_call_parameter_sets.entity_call_parameter_sets:
            lookup_key = ResolvedSpecLookUpKey(
                filter_location=filter_location,
                call_parameter_set=entity_call_parameter_set,
            )
            if spec_resolution_lookup_so_far.spec_resolution_exists(lookup_key):
                logger.info(
                    f"Skipping resolution for {entity_call_parameter_set} at {filter_location} since it has "
                    f"been resolved to:\n"
                    f"{mf_pformat(spec_resolution_lookup_so_far.get_spec_resolutions(lookup_key))}"
                )

            if entity_call_parameter_set not in call_parameter_set_to_spec_pattern:
                call_parameter_set_to_spec_pattern[entity_call_parameter_set] = EntityPattern.from_call_parameter_set(
                    entity_call_parameter_set
                )

        return call_parameter_set_to_spec_pattern

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

            # Need to dedupe as it's possible that the same metric with the same filter is used multiple times to
            # define a derived metric.
            resolved_spec_lookup_so_far = FilterSpecResolutionLookUp.merge_iterable(results_to_merge).dedupe()

            return resolved_spec_lookup_so_far.merge(
                self._resolve_specs_for_where_filters(
                    resolution_node=node,
                    resolution_path=resolution_path,
                    resolved_spec_lookup_so_far=resolved_spec_lookup_so_far,
                    filter_location=WhereFilterLocation.for_metric(node.metric_reference),
                    where_filters=self._get_where_filters_at_metric_node(node),
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
                    resolution_node=node,
                    resolution_path=resolution_path,
                    resolved_spec_lookup_so_far=resolved_spec_lookup_so_far,
                    filter_location=WhereFilterLocation.for_query(node.metrics_in_query),
                    where_filters=node.where_filter_intersection.where_filters,
                )
            )

    @override
    def visit_no_metrics_query_node(self, node: NoMetricsGroupByItemSourceNode) -> FilterSpecResolutionLookUp:
        """Similar to the measure node - filters are applied at the query level."""
        with self._path_from_start_node_tracker.track_node_visit(node):
            return FilterSpecResolutionLookUp.empty_instance()

    def _get_where_filters_at_metric_node(self, metric_node: MetricGroupByItemResolutionNode) -> Sequence[WhereFilter]:
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

        return where_filters

    def _resolve_specs_for_where_filters(
        self,
        resolution_node: ResolutionDagSinkNode,
        resolution_path: MetricFlowQueryResolutionPath,
        resolved_spec_lookup_so_far: FilterSpecResolutionLookUp,
        filter_location: WhereFilterLocation,
        where_filters: Sequence[WhereFilter],
    ) -> FilterSpecResolutionLookUp:
        """Given the filters at a particular node, build the spec lookup for those filters."""
        results_to_merge: List[FilterSpecResolutionLookUp] = []
        resolution_dag = GroupByItemResolutionDag(
            sink_node=resolution_node,
        )
        group_by_item_resolver = GroupByItemResolver(
            manifest_lookup=self._manifest_lookup,
            resolution_dag=resolution_dag,
        )
        call_parameter_set_to_pattern: Dict[CallParameterSet, SpecPattern] = {}
        issue_sets_to_merge: List[MetricFlowQueryResolutionIssueSet] = []
        for where_filter in where_filters:
            try:
                filter_call_parameter_sets = where_filter.call_parameter_sets
            except Exception as e:
                issue_sets_to_merge.append(
                    MetricFlowQueryResolutionIssueSet.from_issue(
                        WhereFilterParsingIssue.from_parameters(
                            where_filter=where_filter,
                            parse_exception=e,
                            query_resolution_path=resolution_path,
                        )
                    )
                )
                continue

            call_parameter_set_to_pattern.update(
                _ResolveWhereFilterSpecVisitor._map_filter_parameter_set_to_pattern(
                    filter_location=filter_location,
                    filter_call_parameter_sets=filter_call_parameter_sets,
                    spec_resolution_lookup_so_far=resolved_spec_lookup_so_far,
                )
            )

        resolutions: List[FilterSpecResolution] = []
        for call_parameter_set, spec_pattern in call_parameter_set_to_pattern.items():
            group_by_item_resolution = group_by_item_resolver.resolve_matching_item_for_filters(
                spec_pattern=spec_pattern,
                resolution_node=resolution_node,
            )
            issue_sets_to_merge.append(group_by_item_resolution.issue_set)
            if group_by_item_resolution.spec is None:
                continue
            resolutions.append(
                FilterSpecResolution(
                    lookup_key=ResolvedSpecLookUpKey(
                        filter_location=filter_location,
                        call_parameter_set=call_parameter_set,
                    ),
                    resolution_path=resolution_path,
                    resolved_spec=group_by_item_resolution.spec,
                )
            )

        results_to_merge.append(
            FilterSpecResolutionLookUp(
                spec_resolutions=tuple(resolutions),
                issue_set=MetricFlowQueryResolutionIssueSet.empty_instance(),
            )
        )

        return FilterSpecResolutionLookUp.merge_iterable(results_to_merge).merge(
            FilterSpecResolutionLookUp(
                spec_resolutions=(),
                issue_set=MetricFlowQueryResolutionIssueSet.merge_iterable(issue_sets_to_merge),
            )
        )
