from __future__ import annotations

import itertools
import logging
from collections import defaultdict
from collections.abc import Mapping, Sequence, Set

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums import MetricType
from metricflow_semantics.errors.error_classes import MetricFlowInternalError
from metricflow_semantics.model.semantics.metric_lookup import MetricLookup
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.where_filter.where_filter_spec_factory import WhereFilterSpecFactory
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.toolkit.mf_graph.formatting.pretty_graph_formatter import PrettyFormatGraphFormatter
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

from metricflow.metric_evaluation.me_plan_table_formatter import MetricEvaluationPlanTableFormatter
from metricflow.metric_evaluation.metric_query_planner import MetricEvaluationPlanner
from metricflow.metric_evaluation.passthrough.base_metric_query_node_builder import (
    BaseMetricQueryNodeBuilder,
)
from metricflow.metric_evaluation.passthrough.me_level_resolver import MetricEvaluationLevelResolver
from metricflow.metric_evaluation.passthrough.node_consolidator import DerivedMetricsNodeConsolidator
from metricflow.metric_evaluation.passthrough.query_set_selector import (
    BestMetricQuerySetSelector,
)
from metricflow.metric_evaluation.plan.me_edges import MetricQueryDependencyEdge
from metricflow.metric_evaluation.plan.me_nodes import (
    DerivedMetricsQueryNode,
    MetricQueryNode,
    TopLevelQueryNode,
)
from metricflow.metric_evaluation.plan.me_plan import (
    MetricEvaluationPlan,
    MutableMetricEvaluationPlan,
)
from metricflow.metric_evaluation.plan.query_element import (
    MetricQueryElement,
    MetricQueryElementCollector,
    MetricQueryElementLookup,
    MetricQueryPropertySet,
)
from metricflow.plan_conversion.node_processor import PredicatePushdownState

logger = logging.getLogger(__name__)


class PassThroughMetricEvaluationPlanner(MetricEvaluationPlanner):
    """Create metric evaluation plans that allow passthrough metrics in queries that compute derived metrics.

    By allowing input metrics to pass through to queries that depend on a derived metric, it may be possible to reduce
    the number of full-outer joins in the final query.

    Using the example query for metrics ['bookings_per_listing', 'bookings', 'views']:

    This planner traverses the metric dependency graph to collect the associated query elements at each evaluation
    level.

    e.g.
        level 0: ['bookings', 'listings', 'views']
        level 1: ['bookings_per_listing']

    Then for each level, it creates a query node that uses the results from the prior levels (using the best one
    to minimize joins) to produce the query elements for the given level.

    e.g.
        Level 0: [
            MetricQueryNode('bookings'),
            MetricQueryNode('listings'),
            MetricQueryNode('views'),
        ]
        Level 1: [
            MetricQueryNode('bookings_per_listing') -> [MetricQueryNode('bookings'), MetricQueryNode('listings')]
        ]

    Nodes that produce derived metrics can pass through query elements that were used as inputs.
    MetricQueryNode('bookings_per_listing') produces 'bookings_per_listing' and passes through 'bookings', 'listings'.

    Finally, a top-level query node is created to get all requested metrics.

    e.g.
        Top-Level: [
            MetricQueryNode('bookings_per_listing') -> MetricQueryNode('bookings_per_listing'),
            MetricQueryNode('bookings') -> MetricQueryNode('bookings_per_listing'),
            MetricQueryNode('views') -> MetricQueryNode('views'),
        ]

    This creates a top-level query that joins 2 input queries:

        MetricQueryNode('bookings_per_listing')
        MetricQueryNode('views')

    instead of a top-level query that joins 3 input queries:

        MetricQueryNode('bookings_per_listing')
        MetricQueryNode('bookings')
        MetricQueryNode('views')
    """

    def __init__(  # noqa: D107
        self,
        manifest_object_lookup: ManifestObjectLookup,
        metric_lookup: MetricLookup,
        column_association_resolver: ColumnAssociationResolver,
    ) -> None:
        super().__init__(
            manifest_object_lookup=manifest_object_lookup,
            metric_lookup=metric_lookup,
            column_association_resolver=column_association_resolver,
        )

        self._level_resolver: MetricEvaluationLevelResolver = MetricEvaluationLevelResolver(manifest_object_lookup)

    @override
    def build_plan(
        self,
        metric_specs: Sequence[MetricSpec],
        group_by_item_specs: Sequence[LinkableInstanceSpec],
        predicate_pushdown_state: PredicatePushdownState,
        filter_spec_factory: WhereFilterSpecFactory,
    ) -> MetricEvaluationPlan:
        # Figure out all forms of metrics that are required.
        logger.debug(
            LazyFormat(
                "Building metric evaluation plan",
                metric_specs=metric_specs,
            )
        )

        # Traverse the metric dependency graph and collect the query elements.
        query_element_collector = MetricQueryElementCollector()
        top_level_query_elements = FrozenOrderedSet(
            (
                MetricQueryElement.create(
                    metric_spec=metric_spec,
                    group_by_item_specs=FrozenOrderedSet(group_by_item_specs),
                    predicate_pushdown_state=predicate_pushdown_state,
                )
                for metric_spec in metric_specs
            )
        )

        for query_element in top_level_query_elements:
            self._recursively_collect_query_elements(
                query_element=query_element,
                query_element_collector=query_element_collector,
                filter_spec_factory=filter_spec_factory,
            )

        query_element_lookup: MetricQueryElementLookup = query_element_collector
        logger.debug(
            LazyFormat(
                "Recursively collected query elements",
                query_element_to_input_elements=query_element_lookup.query_element_to_input_elements,
            )
        )

        # Group the collected query elements by evaluation level.
        level_to_query_elements = self._group_query_elements_by_level(query_element_lookup)

        query_element_to_level: dict[MetricQueryElement, int] = {}
        for level, query_elements_for_current_level in level_to_query_elements.items():
            for query_element in query_elements_for_current_level:
                query_element_to_level[query_element] = level

        logger.debug(LazyFormat("Grouped query elements by level", level_to_query_elements=level_to_query_elements))

        # Create the evaluation plan.
        evaluation_plan = MutableMetricEvaluationPlan.create()

        # Create the nodes for level 0.
        base_metric_query_elements = level_to_query_elements[0]
        base_metric_query_node_builder = BaseMetricQueryNodeBuilder(self._manifest_object_lookup)
        base_metric_query_nodes = base_metric_query_node_builder.build_nodes(base_metric_query_elements)
        logger.debug(
            LazyFormat(
                "Constructed base metric query nodes",
                base_metric_query_nodes=base_metric_query_nodes,
            )
        )
        evaluation_plan.add_nodes(base_metric_query_nodes)

        candidate_query_nodes = list(base_metric_query_nodes)
        max_level = max(level_to_query_elements)

        # For each level > 0, construct the derived metrics query nodes using the results from previous levels.
        for level in range(1, max_level + 1):
            query_elements_for_current_level = level_to_query_elements[level]

            # This generates separate query nodes for each query element.
            output_query_nodes, edges_from_nodes = self._generate_subplan_for_recursive_metrics_at_single_level(
                output_query_elements=query_elements_for_current_level,
                candidate_input_query_nodes=candidate_query_nodes,
                query_element_lookup=query_element_lookup,
                query_element_to_level=query_element_to_level,
            )

            # Since query nodes may have the same sources, consolidate them to reduce the node count.
            node_consolidator = DerivedMetricsNodeConsolidator(
                nodes_to_consolidate=output_query_nodes,
                corresponding_source_edges=edges_from_nodes,
            )
            output_query_nodes, edges_from_nodes = node_consolidator.consolidate_nodes()
            logger.debug(
                LazyFormat(
                    "Resolved recursive metric query nodes",
                    level=level,
                    input_query_nodes=candidate_query_nodes,
                    output_query_nodes=output_query_nodes,
                    edges_from_nodes=edges_from_nodes,
                )
            )
            candidate_query_nodes.extend(output_query_nodes)
            evaluation_plan.add_edges(edges_from_nodes)

        # Generate the node for the top-level query.
        top_level_query_set_selector = BestMetricQuerySetSelector(query_element_to_level)
        top_level_query_set_result = top_level_query_set_selector.find_best_queries(
            desired_query_elements=top_level_query_elements,
            candidate_input_nodes=candidate_query_nodes,
        )

        if top_level_query_set_result.remaining_desired_query_elements:
            raise MetricFlowInternalError(
                LazyFormat(
                    "Unable to get all top-level metrics from candidate queries. This indicates an error in candidate"
                    " query generation.",
                    metric_specs=metric_specs,
                    remaining_desired_query_elements=top_level_query_set_result.remaining_desired_query_elements,
                    candidate_queries=candidate_query_nodes,
                )
            )

        top_level_query_node = TopLevelQueryNode.create(
            passthrough_metric_specs=metric_specs,
            query_properties=MetricQueryPropertySet.create(
                group_by_item_specs=group_by_item_specs,
                predicate_pushdown_state=predicate_pushdown_state,
            ),
        )

        # Generate the source edges for the top-level query node.
        for (
            input_query_node,
            fulfilled_query_elements,
        ) in top_level_query_set_result.input_query_node_to_fulfilled_query_elements.items():
            for query_element in fulfilled_query_elements:
                evaluation_plan.add_edge(
                    MetricQueryDependencyEdge.create(
                        target_node=top_level_query_node,
                        target_node_output_spec=query_element.metric_spec,
                        source_node=input_query_node,
                        source_node_output_spec=query_element.metric_spec,
                    )
                )

        try:
            evaluation_plan.validate()
        except MetricFlowInternalError as e:
            format_result = MetricEvaluationPlanTableFormatter().format_plan(evaluation_plan)
            raise MetricFlowInternalError(
                LazyFormat(
                    "Validation error in the initial passthrough plan",
                    overview_table=format_result.overview_table,
                    node_output_table=format_result.node_output_table,
                )
            ) from e

        logger.debug(
            LazyFormat(
                "Generated initial query graph",
                query_graph=lambda: evaluation_plan.format(PrettyFormatGraphFormatter()),
            )
        )

        # Not all passthrough metrics output by the nodes are required, so remove edges corresponding to the ones that
        # are not used to create a more simple plan.
        node_to_metric_specs_to_retain = self._map_retained_metric_specs_by_node(
            query_graph=evaluation_plan,
            root_query_node=top_level_query_node,
            metric_specs_to_retain_for_root_query_node=set(metric_specs),
        )

        logger.debug(
            LazyFormat(
                "Generated mapping for metric_specs to retain",
                node_to_metric_specs_to_retain=node_to_metric_specs_to_retain,
            )
        )

        pruned_evaluation_plan = self._prune_plan(
            evaluation_plan=evaluation_plan,
            query_node=top_level_query_node,
            node_to_metric_specs_to_retain=node_to_metric_specs_to_retain,
        )

        logger.debug(LazyFormat("Generated pruned evaluation plan", pruned_evaluation_plan=pruned_evaluation_plan))
        return pruned_evaluation_plan

    def _prune_plan(
        self,
        evaluation_plan: MetricEvaluationPlan,
        query_node: MetricQueryNode,
        node_to_metric_specs_to_retain: Mapping[MetricQueryNode, OrderedSet[MetricSpec]],
    ) -> MetricEvaluationPlan:
        """Create a simplified plan that excludes edges that correspond to unused passthrough metrics.

        Unused edges are determined by looking at the metric specs that the given node needs to output, and keeping
        only the source edges required for those specs.
        """
        # Used to keep track of the next node to process in DFS traversal of the evaluation plan.
        nodes_to_process: list[MetricQueryNode] = [query_node]
        # The edges for the new plan.
        new_edges: list[MetricQueryDependencyEdge] = []
        # Maps a node in the input plan to the new plan. Keys represent processed nodes.
        current_node_to_next_node: dict[MetricQueryNode, MetricQueryNode] = {}
        while nodes_to_process:
            current_node = nodes_to_process.pop()

            if current_node in current_node_to_next_node:
                continue

            metric_specs_to_retain_for_current_node = node_to_metric_specs_to_retain.get(current_node)
            if metric_specs_to_retain_for_current_node is None:
                raise MetricFlowInternalError(
                    LazyFormat(
                        "Missing retained metric specs for query node while pruning.",
                        current_node=current_node,
                        known_nodes=tuple(node_to_metric_specs_to_retain),
                    )
                )

            source_edges_for_current_node = evaluation_plan.source_edges(current_node)
            retained_source_edges = tuple(
                source_edge
                for source_edge in source_edges_for_current_node
                if source_edge.target_node_output_spec in metric_specs_to_retain_for_current_node
            )

            if all(edge.source_node in current_node_to_next_node for edge in retained_source_edges):
                next_current_node = current_node.pruned(
                    allowed_specs=metric_specs_to_retain_for_current_node,
                )

                for edge in retained_source_edges:
                    next_source_node = current_node_to_next_node[edge.head_node]
                    new_edges.append(
                        MetricQueryDependencyEdge.create(
                            target_node=next_current_node,
                            target_node_output_spec=edge.target_node_output_spec,
                            source_node=next_source_node,
                            source_node_output_spec=edge.source_node_output_spec,
                        )
                    )
                current_node_to_next_node[current_node] = next_current_node
                continue

            nodes_to_process.append(current_node)
            # Since the collected query elements were deduplicated, there shouldn't be any source nodes that are not
            # retained. Defensively traverse only retained dependencies, but add a log message.
            source_nodes_for_retained_edges = FrozenOrderedSet(
                edge.source_node for edge in reversed(retained_source_edges)
            )
            source_nodes_for_current_node = evaluation_plan.source_nodes(current_node)
            if source_nodes_for_retained_edges != source_nodes_for_current_node:
                logger.error(
                    LazyFormat(
                        "Not all source nodes retained. While this may not cause issues, this is a bug and should be"
                        " investigated.",
                        source_nodes_for_retained_edges=source_nodes_for_retained_edges,
                        source_nodes_for_current_node=source_nodes_for_current_node,
                    )
                )
            nodes_to_process.extend(source_nodes_for_retained_edges)

        new_graph = MutableMetricEvaluationPlan.create()
        new_graph.add_edges(new_edges)

        try:
            new_graph.validate()
        except MetricFlowInternalError as e:
            format_result = MetricEvaluationPlanTableFormatter().format_plan(new_graph)
            raise MetricFlowInternalError(
                LazyFormat(
                    "Error in pruned plan",
                    overview_table=format_result.overview_table,
                    node_output_table=format_result.node_output_table,
                )
            ) from e
        return new_graph

    def _map_retained_metric_specs_by_node(
        self,
        query_graph: MetricEvaluationPlan,
        root_query_node: MetricQueryNode,
        metric_specs_to_retain_for_root_query_node: Set[MetricSpec],
    ) -> Mapping[MetricQueryNode, OrderedSet[MetricSpec]]:
        """Map each node to the output metric specs required to satisfy retained outputs at the root."""
        nodes_to_process: MutableOrderedSet[MetricQueryNode] = MutableOrderedSet([root_query_node])
        node_to_metric_specs_to_retain: defaultdict[MetricQueryNode, MutableOrderedSet[MetricSpec]] = defaultdict(
            MutableOrderedSet
        )
        node_to_metric_specs_to_retain[root_query_node].update(metric_specs_to_retain_for_root_query_node)

        while nodes_to_process:
            current_node = nodes_to_process.pop()

            metric_specs_to_retain_for_current_node = node_to_metric_specs_to_retain[current_node]
            for edge in query_graph.source_edges(current_node):
                required_output_metric_spec = edge.target_node_output_spec

                if required_output_metric_spec in metric_specs_to_retain_for_current_node:
                    node_to_metric_specs_to_retain[edge.head_node].add(edge.source_node_output_spec)
                    nodes_to_process.add(edge.head_node)

        return {node: retained_specs.as_frozen() for node, retained_specs in node_to_metric_specs_to_retain.items()}

    @staticmethod
    def _metric_spec_allows_passthrough(metric_spec: MetricSpec) -> bool:
        """Return whether a metric spec can be safely passed through by a derived metric query node."""
        return metric_spec.offset_window is None and metric_spec.offset_to_grain is None and metric_spec.alias is None

    def _generate_subplan_for_recursive_metrics_at_single_level(
        self,
        output_query_elements: OrderedSet[MetricQueryElement],
        candidate_input_query_nodes: Sequence[MetricQueryNode],
        query_element_lookup: MetricQueryElementLookup,
        query_element_to_level: Mapping[MetricQueryElement, int],
    ) -> tuple[OrderedSet[DerivedMetricsQueryNode], OrderedSet[MetricQueryDependencyEdge]]:
        """For a given evaluation level, generate the nodes and edges to build `output_query_elements`.

        This uses a greedy algorithm to use the best / biggest input nodes to build a node that computes each output
        query element. A separate node is created for each output element, and a different method is used
        to consolidate them (e.g. if 2 derived metrics use the same sources, a single node could compute both).
        """
        # Sort to fulfill the metric with the largest number of inputs.
        sorted_output_query_elements = sorted(
            output_query_elements,
            key=lambda query_element: len(query_element_lookup.get_input_query_elements(query_element)),
            reverse=True,
        )

        query_set_selector = BestMetricQuerySetSelector(query_element_to_level)

        nodes: MutableOrderedSet[DerivedMetricsQueryNode] = MutableOrderedSet()
        edges: MutableOrderedSet[MetricQueryDependencyEdge] = MutableOrderedSet()

        for output_query_element in sorted_output_query_elements:
            input_query_elements = query_element_lookup.get_input_query_elements(output_query_element)

            find_best_query_set_result = query_set_selector.find_best_queries(
                desired_query_elements=input_query_elements,
                candidate_input_nodes=candidate_input_query_nodes,
            )

            if find_best_query_set_result.remaining_desired_query_elements:
                raise MetricFlowInternalError(
                    LazyFormat(
                        "Unable to find a query element group that can satisfy the inputs",
                        input_query_elements=input_query_elements,
                        best_query_element_group_result=find_best_query_set_result,
                    )
                )

            passthrough_metric_specs: FrozenOrderedSet[MetricSpec] = FrozenOrderedSet()
            computed_metric_spec = output_query_element.metric_spec

            if self._metric_spec_allows_passthrough(computed_metric_spec):
                passthrough_metric_specs = FrozenOrderedSet(
                    passthrough_metric_spec
                    for passthrough_metric_spec in itertools.chain.from_iterable(
                        input_node.output_metric_specs
                        for input_node in find_best_query_set_result.input_query_node_to_fulfilled_query_elements
                    )
                    if self._metric_spec_allows_passthrough(passthrough_metric_spec)
                )
            derived_metrics_node = DerivedMetricsQueryNode.create(
                computed_metric_specs=[output_query_element.metric_spec],
                passthrough_metric_specs=passthrough_metric_specs,
                query_properties=output_query_element.query_properties,
            )

            nodes.add(derived_metrics_node)

            passthrough_metric_specs_with_edge_added: MutableOrderedSet[MetricSpec] = MutableOrderedSet()
            for (
                input_query_node,
                fulfilled_query_elements,
            ) in find_best_query_set_result.input_query_node_to_fulfilled_query_elements.items():
                for input_metric_spec in input_query_node.output_metric_specs:
                    if input_metric_spec in passthrough_metric_specs:
                        edges.add(
                            MetricQueryDependencyEdge.create(
                                target_node=derived_metrics_node,
                                target_node_output_spec=input_metric_spec,
                                source_node=input_query_node,
                                source_node_output_spec=input_metric_spec,
                            )
                        )
                        passthrough_metric_specs_with_edge_added.add(input_metric_spec)
                for fulfilled_query_element in fulfilled_query_elements:
                    edges.add(
                        MetricQueryDependencyEdge.create(
                            target_node=derived_metrics_node,
                            target_node_output_spec=output_query_element.metric_spec,
                            source_node=input_query_node,
                            source_node_output_spec=fulfilled_query_element.metric_spec,
                        )
                    )

            missing_passthrough_specs = passthrough_metric_specs.difference(passthrough_metric_specs_with_edge_added)
            if len(missing_passthrough_specs) > 0:
                raise MetricFlowInternalError(
                    LazyFormat("Missing passthrough spec in edges", missing_passthrough_specs=missing_passthrough_specs)
                )
        return nodes, edges

    def _recursively_collect_query_elements(
        self,
        query_element: MetricQueryElement,
        query_element_collector: MetricQueryElementCollector,
        filter_spec_factory: WhereFilterSpecFactory,
    ) -> None:
        """Traverse the metric dependency graph for the query element and recursively collect input elements."""
        if query_element in query_element_collector.query_elements:
            return

        metric_spec = query_element.metric_spec
        metric_name = metric_spec.element_name
        metric = self._manifest_object_lookup.get_metric(metric_name)
        metric_type = metric.type

        if (
            metric_type is MetricType.SIMPLE
            or metric_type is MetricType.CUMULATIVE
            or metric_type is MetricType.CONVERSION
        ):
            query_element_collector.add_query_element(query_element=query_element, input_query_elements=None)
        elif metric_type is MetricType.RATIO or metric_type is MetricType.DERIVED:
            additional_filter_specs = metric_spec.where_filter_specs
            group_by_item_specs_for_inputs: OrderedSet[LinkableInstanceSpec] = query_element.group_by_item_specs
            predicate_pushdown_state_for_inputs = query_element.predicate_pushdown_state

            if metric_spec.has_time_offset:
                group_by_item_specs_for_inputs = self._query_helper.resolve_group_by_specs_for_time_offset_metric_input(
                    queried_group_by_specs=query_element.group_by_item_specs,
                    filter_specs=metric_spec.where_filter_specs,
                )
                predicate_pushdown_state_for_inputs = PredicatePushdownState.with_pushdown_disabled()
                # If metric is offset, we'll apply where constraint after offset to avoid removing values
                # unexpectedly. Time constraint will be applied by INNER JOINing to time spine.
                # We may consider encapsulating this in pushdown state later, but as of this moment pushdown
                # is about post-join to pre-join for dimension access, and relies on the builder to collect
                # predicates from query and metric specs and make them available at simple-metric-input level.
                additional_filter_specs = ()

            input_metric_specs = self._build_input_metric_specs_for_derived_metric(
                metric_name=metric_name,
                filter_spec_factory=filter_spec_factory,
                additional_filter_specs=additional_filter_specs,
            )

            input_query_elements: list[MetricQueryElement] = []
            for input_metric_spec in input_metric_specs:
                input_query_element = MetricQueryElement.create(
                    metric_spec=input_metric_spec,
                    group_by_item_specs=group_by_item_specs_for_inputs,
                    predicate_pushdown_state=predicate_pushdown_state_for_inputs,
                )
                input_query_elements.append(input_query_element)

                self._recursively_collect_query_elements(
                    query_element=input_query_element,
                    query_element_collector=query_element_collector,
                    filter_spec_factory=filter_spec_factory,
                )

            query_element_collector.add_query_element(query_element, input_query_elements=input_query_elements)
        else:
            assert_values_exhausted(metric_type)

    def _group_query_elements_by_level(
        self, query_element_lookup: MetricQueryElementLookup
    ) -> Mapping[int, OrderedSet[MetricQueryElement]]:
        """Group query elements by metric evaluation level in ascending order."""
        level_to_query_elements: defaultdict[int, MutableOrderedSet[MetricQueryElement]] = defaultdict(
            MutableOrderedSet
        )

        for query_element in query_element_lookup.query_elements:
            level_to_query_elements[self._level_resolver.resolve_evaluation_level(query_element.metric_name)].add(
                query_element
            )

        sorted_level_to_query_elements: dict[int, FrozenOrderedSet[MetricQueryElement]] = {}
        for level in sorted(level_to_query_elements):
            sorted_level_to_query_elements[level] = FrozenOrderedSet(level_to_query_elements[level])

        if not sorted_level_to_query_elements:
            raise MetricFlowInternalError("Expected at least one query element when grouping by level.")

        # Levels should be contiguous from 0 to max_level.
        sorted_levels = tuple(sorted_level_to_query_elements)
        max_level = sorted_levels[-1]
        expected_levels = tuple(range(max_level + 1))
        if sorted_levels != expected_levels:
            raise MetricFlowInternalError(
                LazyFormat(
                    "Expected evaluation levels to be contiguous from 0.",
                    sorted_levels=sorted_levels,
                    expected_levels=expected_levels,
                )
            )

        return sorted_level_to_query_elements
