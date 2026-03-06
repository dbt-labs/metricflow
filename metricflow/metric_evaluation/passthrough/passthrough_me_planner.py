from __future__ import annotations

import itertools
import logging
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence, Set

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums import MetricType
from metricflow_semantics.errors.error_classes import MetricFlowInternalError
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.where_filter.where_filter_spec_factory import WhereFilterSpecFactory
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_graph.formatting.pretty_graph_formatter import PrettyFormatGraphFormatter
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

from metricflow.metric_evaluation.metric_query_planner import MetricEvaluationPlanner
from metricflow.metric_evaluation.passthrough.me_level_resolver import MetricEvaluationLevelResolver
from metricflow.metric_evaluation.passthrough.query_element_group_selector import (
    BestMetricQuerySetSelector,
    FindBestQuerySetResult,
)
from metricflow.metric_evaluation.passthrough.query_element_grouper import QueryElementGrouper
from metricflow.metric_evaluation.plan.me_edges import MetricQueryDependencyEdge
from metricflow.metric_evaluation.plan.me_nodes import (
    ConversionMetricQueryNode,
    CumulativeMetricQueryNode,
    DerivedMetricsQueryNode,
    MetricQueryNode,
    SimpleMetricsQueryNode,
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
    """Plans metric evaluation with passthrough metrics for derived metrics.

    By allowing input metrics to pass through to queries that depend on a derived metric, it may be possible to reduce
    the number of full-outer joins in the top-level query.
    """

    def __init__(  # noqa: D107
        self, manifest_object_lookup: ManifestObjectLookup, column_association_resolver: ColumnAssociationResolver
    ) -> None:
        super().__init__(
            manifest_object_lookup=manifest_object_lookup, column_association_resolver=column_association_resolver
        )

        self._level_resolver: MetricEvaluationLevelResolver = MetricEvaluationLevelResolver(manifest_object_lookup)
        self._query_element_grouper = QueryElementGrouper(manifest_object_lookup)

    @override
    def build_plan(
        self,
        metric_specs: Sequence[MetricSpec],
        group_by_item_specs: Sequence[LinkableInstanceSpec],
        predicate_pushdown_state: PredicatePushdownState,
        filter_spec_factory: WhereFilterSpecFactory,
    ) -> MetricEvaluationPlan:
        # Figure out all forms of metrics that are required.
        query_element_collector = MetricQueryElementCollector()
        logger.debug(
            LazyFormat(
                "Building metric evaluation plan",
                metric_specs=metric_specs,
            )
        )
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

        # Group them by evaluation level
        level_to_query_elements = self._group_query_elements_by_level(query_element_lookup)
        # Sanity check that levels are 0 to N
        assert min(level_to_query_elements) == 0
        max_level = max(level_to_query_elements)
        assert tuple(level_to_query_elements) == tuple(range(max_level + 1))

        query_element_to_level: dict[MetricQueryElement, int] = {}
        for level, query_elements_for_current_level in level_to_query_elements.items():
            for query_element in query_elements_for_current_level:
                query_element_to_level[query_element] = level

        logger.debug(LazyFormat("Grouped query elements by level", level_to_query_elements=level_to_query_elements))

        base_metric_query_elements = level_to_query_elements[0]

        query_graph = MutableMetricEvaluationPlan.create()

        base_metric_query_nodes = self._get_nodes_for_base_metrics(base_metric_query_elements)

        logger.debug(
            LazyFormat(
                "Resolved base metric query nodes",
                base_metric_query_nodes=base_metric_query_nodes,
            )
        )
        query_graph.add_nodes(base_metric_query_nodes)

        candidate_query_nodes = list(base_metric_query_nodes)

        for level in range(1, max_level + 1):
            query_elements_for_current_level = level_to_query_elements[level]
            output_query_nodes, edges_from_nodes = self._generate_subplan_for_recursive_metrics_at_single_level(
                output_query_elements=query_elements_for_current_level,
                candidate_input_query_nodes=candidate_query_nodes,
                query_element_lookup=query_element_lookup,
                query_element_to_level=query_element_to_level,
            )
            logger.debug(
                LazyFormat(
                    "Resolved recursive metric query nodes",
                    level=level,
                    metric_spec=metric_specs,
                    input_query_nodes=candidate_query_nodes,
                    output_query_nodes=output_query_nodes,
                )
            )
            candidate_query_nodes.extend(output_query_nodes)
            query_graph.add_edges(edges_from_nodes)

        query_set_selector_for_top_level = BestMetricQuerySetSelector(query_element_to_level)
        find_best_query_set_result = query_set_selector_for_top_level.find_best_queries(
            desired_query_elements=top_level_query_elements,
            candidate_input_nodes=candidate_query_nodes,
        )

        if find_best_query_set_result.remaining_desired_query_elements:
            raise RuntimeError(
                LazyFormat(
                    "Unable to get all top-level metrics from candidate queries. This indicates an error in candidate"
                    " query generation.",
                    metric_specs=metric_specs,
                    remaining_desired_query_elements=find_best_query_set_result.remaining_desired_query_elements,
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

        for (
            input_query_node,
            fulfilled_query_elements,
        ) in find_best_query_set_result.input_query_node_to_fulfilled_query_elements.items():
            for query_element in fulfilled_query_elements:
                query_graph.add_edge(
                    MetricQueryDependencyEdge.create(
                        target_node=top_level_query_node,
                        target_node_output_spec=query_element.metric_spec,
                        source_node=input_query_node,
                        source_node_output_spec=query_element.metric_spec,
                    )
                )
        query_graph.validate()

        logger.debug(
            LazyFormat(
                "Generated initial query graph", query_graph=lambda: query_graph.format(PrettyFormatGraphFormatter())
            )
        )

        node_to_metric_specs_to_retain = self._map_retained_metric_specs_by_node(
            query_graph=query_graph,
            root_query_node=top_level_query_node,
            metric_specs_to_retain_for_root_query_node=set(metric_specs),
        )

        logger.debug(
            LazyFormat(
                "Generated mapping for metric_specs to retain",
                node_to_metric_specs_to_retain=node_to_metric_specs_to_retain,
            )
        )

        pruned_graph = self._prune_graph(
            query_graph=query_graph,
            query_node=top_level_query_node,
            node_to_metric_specs_to_retain=node_to_metric_specs_to_retain,
        )

        logger.debug(
            LazyFormat("Generated pruned graph", pruned_graph=lambda: pruned_graph.format(PrettyFormatGraphFormatter()))
        )

        return pruned_graph

    def _prune_graph(
        self,
        query_graph: MetricEvaluationPlan,
        query_node: MetricQueryNode,
        node_to_metric_specs_to_retain: Mapping[MetricQueryNode, OrderedSet[MetricSpec]],
    ) -> MetricEvaluationPlan:
        nodes_to_process: list[MetricQueryNode] = [query_node]
        new_edges: list[MetricQueryDependencyEdge] = []
        current_node_to_next_node: dict[MetricQueryNode, MetricQueryNode] = {}
        while nodes_to_process:
            current_node = nodes_to_process.pop()

            if current_node in current_node_to_next_node:
                continue

            source_edges_for_current_node = query_graph.source_edges(current_node)

            if all(edge.source_node in current_node_to_next_node for edge in source_edges_for_current_node):
                metric_specs_to_retain = node_to_metric_specs_to_retain[current_node]
                retained_edges = (
                    edge
                    for edge in source_edges_for_current_node
                    if edge.target_node_output_spec in metric_specs_to_retain
                )
                next_current_node = current_node.pruned(
                    allowed_specs=node_to_metric_specs_to_retain[current_node],
                )

                for edge in retained_edges:
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
            nodes_to_process.extend(edge.source_node for edge in source_edges_for_current_node)
            # for edge in source_edges_for_current_node:
            #     nodes_to_process.append(edge.head_node)

        new_graph = MutableMetricEvaluationPlan.create()
        new_graph.add_edges(new_edges)

        new_graph.validate()
        return new_graph

    def _map_retained_metric_specs_by_node(
        self,
        query_graph: MetricEvaluationPlan,
        root_query_node: MetricQueryNode,
        metric_specs_to_retain_for_root_query_node: Set[MetricSpec],
    ) -> Mapping[MetricQueryNode, OrderedSet[MetricSpec]]:
        nodes_to_process: MutableOrderedSet[MetricQueryNode] = MutableOrderedSet([root_query_node])
        node_to_metric_specs_to_retain: defaultdict[MetricQueryNode, MutableOrderedSet[MetricSpec]] = defaultdict(
            MutableOrderedSet
        )
        node_to_metric_specs_to_retain[root_query_node].update(metric_specs_to_retain_for_root_query_node)

        while nodes_to_process:
            current_node = nodes_to_process.pop()

            metric_specs_to_retain_for_current_node = node_to_metric_specs_to_retain[current_node]
            for edge in query_graph.edges_with_tail_node(current_node):
                tail_node_metric_spec = edge.target_node_output_spec

                if tail_node_metric_spec in metric_specs_to_retain_for_current_node:
                    node_to_metric_specs_to_retain[edge.head_node].add(edge.source_node_output_spec)
                    nodes_to_process.add(edge.head_node)

        return node_to_metric_specs_to_retain

    def _get_nodes_for_base_metrics(self, query_elements: Iterable[MetricQueryElement]) -> Sequence[MetricQueryNode]:
        nodes: list[MetricQueryNode] = []

        grouping_result = self._query_element_grouper.group_query_elements(query_elements)

        for property_set, query_elements in grouping_result.property_set_to_simple_metric_query_elements.items():
            logger.debug(
                LazyFormat(
                    "Found simple metric group",
                    property_set=property_set,
                    query_elements=query_elements,
                )
            )
            nodes.append(
                SimpleMetricsQueryNode.create(
                    model_id=property_set.model_id,
                    metric_specs=(query_element.metric_spec for query_element in query_elements),
                    query_properties=property_set,
                )
            )

        for non_grouped_query_element in grouping_result.non_grouped_query_elements:
            metric_name = non_grouped_query_element.metric_spec.element_name
            metric_type = self._manifest_object_lookup.get_metric(metric_name).type

            if metric_type is MetricType.SIMPLE:
                nodes.append(
                    SimpleMetricsQueryNode.create(
                        model_id=self._manifest_object_lookup.simple_metric_name_to_input[metric_name].model_id,
                        metric_specs=(non_grouped_query_element.metric_spec,),
                        query_properties=non_grouped_query_element.query_properties,
                    )
                )
            elif metric_type is MetricType.CUMULATIVE:
                nodes.append(
                    CumulativeMetricQueryNode.create(
                        metric_spec=non_grouped_query_element.metric_spec,
                        query_properties=non_grouped_query_element.query_properties,
                    )
                )
            elif metric_type is MetricType.CONVERSION:
                nodes.append(
                    ConversionMetricQueryNode.create(
                        metric_spec=non_grouped_query_element.metric_spec,
                        query_properties=non_grouped_query_element.query_properties,
                    )
                )
            elif metric_type is MetricType.RATIO or metric_type is MetricType.DERIVED:
                raise MetricFlowInternalError(
                    LazyFormat(
                        "Only base metrics should have been provided to this method",
                        non_grouped_query_element=non_grouped_query_element,
                    )
                )

        return nodes

    def _metric_spec_allows_passthrough(self, metric_spec: MetricSpec) -> bool:
        return metric_spec.offset_window is None and metric_spec.offset_to_grain is None and metric_spec.alias is None

    def _generate_subplan_for_recursive_metrics_at_single_level(
        self,
        output_query_elements: OrderedSet[MetricQueryElement],
        candidate_input_query_nodes: Sequence[MetricQueryNode],
        query_element_lookup: MetricQueryElementLookup,
        query_element_to_level: Mapping[MetricQueryElement, int],
    ) -> tuple[OrderedSet[MetricQueryNode], OrderedSet[MetricQueryDependencyEdge]]:
        # Sort to fulfill the metric with the largest number of inputs to use a greedy algorithm.
        sorted_output_query_elements = sorted(
            output_query_elements,
            key=lambda query_element: len(query_element_lookup.get_input_query_elements(query_element)),
            reverse=True,
        )

        query_selector = BestMetricQuerySetSelector(query_element_to_level)

        output_query_element_to_best_query_result: dict[MetricQueryElement, FindBestQuerySetResult] = {}

        nodes: MutableOrderedSet[DerivedMetricsQueryNode] = MutableOrderedSet()
        edges: MutableOrderedSet[MetricQueryDependencyEdge] = MutableOrderedSet()

        for output_query_element in sorted_output_query_elements:
            input_query_elements = query_element_lookup.get_input_query_elements(output_query_element)

            find_best_query_set_result = query_selector.find_best_queries(
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
            output_query_element_to_best_query_result[output_query_element] = find_best_query_set_result

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
                raise RuntimeError(
                    LazyFormat("Missing passthrough spec in edges", missing_passthrough_specs=missing_passthrough_specs)
                )
        # derived_metric_grouper = DerivedMetricSourceGrouper(
        #     derived_metric_nodes=nodes,
        #     source_edges=edges,
        # )
        # new_nodes, new_edges = derived_metric_grouper.group_nodes()
        # return (new_nodes, new_edges)
        return nodes, edges

        # input_key_to_output_query_elements: defaultdict[DerivedMetricInputKey, list[MetricQueryElement]] = defaultdict(
        #     list
        # )
        # for output_query_element, best_query_result in output_query_element_to_best_query_result.items():
        #     input_key_to_output_query_elements[
        #         DerivedMetricInputKey(
        #             input_query_nodes=FrozenOrderedSet(best_query_result.input_query_node_to_fulfilled_query_elements),
        #             query_properties=output_query_element.query_properties,
        #         )
        #     ].append(output_query_element)
        #
        # # Group derived metrics by common input nodes.
        # nodes_to_add: list[MetricQueryNode] = []
        # edges_to_add: list[MetricQueryDependencyEdge] = []
        #
        # for input_key, output_query_elements_for_input_key in input_key_to_output_query_elements.items():
        #     computed_metric_specs: Iterable[MetricSpec] = (
        #         output_query_element.metric_spec for output_query_element in output_query_elements_for_input_key
        #     )
        #     passthrough_metric_specs: Iterable[MetricSpec] = ()
        #
        #     if all(self._metric_spec_allows_passthrough(metric_spec) for metric_spec in computed_metric_specs):
        #         passthrough_metric_specs = itertools.chain.from_iterable(
        #             input_query_node.output_metric_specs for input_query_node in input_key.input_query_nodes
        #         )
        #
        #     # TODO: Consider removal.
        #     for output_query_element in output_query_elements_for_input_key:
        #         best_query_result = output_query_element_to_best_query_result[output_query_element]
        #         assert frozenset(best_query_result.input_query_node_to_fulfilled_query_elements) == input_key
        #
        #     derived_metric_node = DerivedMetricsQueryNode.create(
        #         computed_metric_specs=computed_metric_specs,
        #         passthrough_metric_specs=passthrough_metric_specs,
        #         query_properties=input_key.query_properties,
        #     )
        #     nodes_to_add.append(derived_metric_node)
        #
        #     for output_query_element in output_query_elements:
        #         best_query_result = output_query_element_to_best_query_result[output_query_element]
        #         for (
        #             input_query_node,
        #             fulfilled_query_elements,
        #         ) in best_query_result.input_query_node_to_fulfilled_query_elements.items():
        #             for fulfilled_query_element in fulfilled_query_elements:
        #                 edges_to_add.append(
        #                     MetricQueryDependencyEdge.create(
        #                         target_node=derived_metric_node,
        #                         target_node_output_spec=output_query_element.metric_spec,
        #                         source_node=input_query_node,
        #                         source_node_output_spec=fulfilled_query_element.metric_spec,
        #                     )
        #                 )

        return (nodes, edges)

    # def _consolidate_nodes(
    #         self, nodes: Sequence[DerivedMetricsQueryNode], edges: Sequence[MetricQueryDependencyEdge]
    # ) -> tuple[Sequence[MetricQueryNode], Sequence[MetricQueryDependencyEdge]]:
    #
    #     nodes_that_can_be_consolidated: MutableOrderedSet[DerivedMetricsQueryNode] = MutableOrderedSet()
    #     nodes_that_cant_be_consolidated: MutableOrderedSet [DerivedMetricsQueryNode] = MutableOrderedSet()
    #     for node in nodes:
    #         if all(metric_spec.alias is None for metric_spec in node.output_metric_specs):
    #             nodes_that_can_be_consolidated.add(node)
    #         else:
    #             nodes_that_cant_be_consolidated.add(node)
    #
    #     edges_for_consolidation = (
    #         edge for edge in edges if edge.target_node in nodes_that_can_be_consolidated
    #     )
    #     subplan = MutableMetricEvaluationPlan.create()
    #     subplan.add_edges(edges_for_consolidation)
    #
    #     target_node_to_source_node_set: dict[DerivedMetricsQueryNode, FrozenOrderedSet[MetricQueryNode]] = {}
    #
    #     for node in nodes_that_can_be_consolidated:
    #         target_node_to_source_node_set[node] = subplan.source_nodes(node).as_frozen()
    #
    #     source_node_set_to_target_nodes: defaultdict[FrozenOrderedSet[MetricQueryNode], MutableOrderedSet[DerivedMetricsQueryNode]] = defaultdict(MutableOrderedSet)
    #
    #     for target_node, source_node_set in target_node_to_source_node_set.items():
    #         source_node_set_to_target_nodes[source_node_set].add(target_node)
    #
    #     consolidated_nodes: list[DerivedMetricsQueryNode] = []
    #     for source_node_set, target_nodes in source_node_set_to_target_nodes.items():
    #         computed_metric_specs = FrozenOrderedSet(
    #             itertools.chain.from_iterable(target_node.computed_metric_specs for target_node in target_nodes)
    #         )
    #         passthrough_metric_specs = FrozenOrderedSet(
    #             itertools.chain.from_iterable(target_node.passthrough_metric_specs for target_node in target_nodes)
    #         )
    #
    #         collected_query
    #         consolidated_nodes.append(
    #             DerivedMetricsQueryNode.create(
    #                 computed_metric_specs=computed_metric_specs,
    #                 passthrough_metric_specs=passthrough_metric_specs,
    #                 query_properties=
    #             )
    #         )

    def _recursively_collect_query_elements(
        self,
        query_element: MetricQueryElement,
        query_element_collector: MetricQueryElementCollector,
        filter_spec_factory: WhereFilterSpecFactory,
    ) -> None:
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
                group_by_item_specs_for_inputs = self._required_group_by_items_for_inputs_to_a_time_offset_metric(
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

        return sorted_level_to_query_elements


@fast_frozen_dataclass()
class DerivedMetricInputKey:
    input_query_nodes: FrozenOrderedSet[MetricQueryNode]
    # TODO: Check if `query_properties` are needed.
    query_properties: MetricQueryPropertySet
