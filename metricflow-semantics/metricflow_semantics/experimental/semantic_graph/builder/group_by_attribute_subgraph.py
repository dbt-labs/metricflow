from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, Optional

from dbt_semantic_interfaces.type_enums import TimeGranularity
from typing_extensions import override

from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.experimental.semantic_graph.edges.edge_labels import MetricDefinitionLabel
from metricflow_semantics.experimental.semantic_graph.edges.entity_relationship import (
    EntityRelationshipEdge,
)
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import (
    GroupByAttributeRootNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    GroupByAttributeLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder import (
    MetricflowGraphPathFinder,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.weight_function import (
    WeightFunction,
)
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph, SemanticGraph
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


class GroupByAttributeSubgraphGenerator:
    # TODO: Replace this value with something more appropriate.
    _MAX_SEARCH_DEPTH = 10

    def __init__(
        self,
        semantic_graph: SemanticGraph,
        path_finder: MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath],
    ) -> None:
        self._semantic_graph = semantic_graph
        self._path_finder = path_finder
        self._mutable_path = AttributeRecipeWriterPath.create()
        self._verbose_debug_logs = True

    # def _copy_descendants(
    #     self,
    #     subgraph: MutableSemanticGraph,
    #     source_nodes: OrderedSet[SemanticGraphNode],
    #     candidate_target_nodes: OrderedSet[SemanticGraphNode],
    # ) -> None:
    #     descendant_subgraph_edges = self._path_finder.descendant_edges(
    #         graph=self._semantic_graph,
    #         source_nodes=source_nodes,
    #         candidate_target_nodes=candidate_target_nodes,
    #     )
    #
    #     for descendant_subgraph_edge in descendant_subgraph_edges:
    #         subgraph.add_edge(descendant_subgraph_edge)
    #
    # def _find_nearest_measure_nodes(self, source_node: SemanticGraphNode) -> FrozenOrderedSet[SemanticGraphNode]:
    #     measure_nodes = self._semantic_graph.nodes_with_label(MeasureAttributeLabel.get_instance(measure_name=None))
    #     result = self._path_finder.find_reachable_targets_simple(
    #         graph=self._semantic_graph,
    #         mutable_path=self._mutable_path,
    #         source_node=source_node,
    #         candidate_target_nodes=measure_nodes,
    #         max_path_weight=GroupByAttributeSubgraphGenerator._MAX_SEARCH_DEPTH,
    #         weight_function=BlockMetricDefinitionEdgeWeightFunction(),
    #     )
    #     return result.reachable_targets
    #
    # def _find_successors_for_group_by_attribute_node(
    #     self,
    #     source_nodes: OrderedSet[SemanticGraphNode],
    # ) -> FrozenOrderedSet[SemanticGraphNode]:
    #     successor_nodes = MutableOrderedSet[SemanticGraphNode]()
    #     candidate_target_nodes = MutableOrderedSet[SemanticGraphNode]()
    #     candidate_target_nodes.update(self._semantic_graph.nodes_with_label(GroupByAttributeLabel.get_instance()))
    #     candidate_target_nodes.update(self._semantic_graph.nodes_with_label(DsiEntityLabel.get_instance()))
    #     candidate_target_nodes.update(self._semantic_graph.nodes_with_label(MetricTimeLabel.get_instance()))
    #
    #     for i, source_node in enumerate(source_nodes):
    #         result = self._path_finder.find_reachable_targets_simple(
    #             graph=self._semantic_graph,
    #             mutable_path=self._mutable_path,
    #             source_node=source_node,
    #             candidate_target_nodes=candidate_target_nodes,
    #             max_path_weight=GroupByAttributeSubgraphGenerator._MAX_SEARCH_DEPTH,
    #             weight_function=BlockMetricDefinitionEdgeWeightFunction(),
    #         )
    #
    #         if i == 0:
    #             successor_nodes = result.reachable_targets
    #         else:
    #             successor_nodes = successor_nodes.intersection(result.reachable_targets)
    #
    #     return successor_nodes
    #
    # def _find_nearest_aggregation_node(self, source_node: SemanticGraphNode) -> SemanticGraphNode:
    #     aggregation_nodes = self._semantic_graph.nodes_with_label(AggregationLabel.get_instance())
    #     semantic_model_nodes = self._semantic_graph.nodes_with_label(SemanticModelLabel.get_instance())
    #     result = self._path_finder.find_reachable_targets_simple(
    #         graph=self._semantic_graph,
    #         mutable_path=self._mutable_path,
    #         source_node=source_node,
    #         candidate_target_nodes=aggregation_nodes.union(semantic_model_nodes),
    #         max_path_weight=GroupByAttributeSubgraphGenerator._MAX_SEARCH_DEPTH,
    #         weight_function=BlockMetricDefinitionEdgeWeightFunction(),
    #     )
    #     found_aggregation_nodes = result.reachable_targets.intersection(aggregation_nodes)
    #     if len(result.reachable_targets) != 1:
    #         raise RuntimeError(
    #             LazyFormat(
    #                 "Did not find exactly one aggregation node. This indicates an error in graph" " construction.",
    #                 source_node=source_node,
    #                 found_aggregation_nodes=found_aggregation_nodes,
    #             )
    #         )
    #
    #     return mf_first_item(found_aggregation_nodes)
    #
    # def generate_subgraph2(self, source_node: SemanticGraphNode) -> AttributeSubgraphResult:
    #     nearest_measure_nodes = self._find_nearest_measure_nodes(source_node)
    #
    #     for measure_node in nearest_measure_nodes:
    #         raise NotImplementedError

    def generate_subgraph(self, source_node: SemanticGraphNode) -> AttributeSubgraphResult:
        path_finder = self._path_finder
        semantic_graph = self._semantic_graph

        # Find semantic model nodes.
        raise NotImplementedError

    def generate_subgraph2(self, source_node: SemanticGraphNode) -> AttributeSubgraphResult:
        path_finder = self._path_finder
        semantic_graph = self._semantic_graph
        subgraph = MutableSemanticGraph.create()

        min_time_grains: list[Optional[TimeGranularity]] = [source_node.recipe_update.add_min_time_grain]
        derivative_source_model_ids = MutableOrderedSet[SemanticModelId]()
        model_id = source_node.recipe_update.join_model
        if model_id is not None:
            derivative_source_model_ids.add(model_id)

        group_by_attribute_nodes = semantic_graph.nodes_with_label(GroupByAttributeLabel.get_instance())

        # for edge_from_source_node in semantic_graph.edges_with_tail_node(source_node):
        #     min_time_grains.append(edge_from_source_node.attribute_recipe_update.add_min_time_grain)
        #     min_time_grains.append(edge_from_source_node.head_node.attribute_recipe_update.add_min_time_grain)
        #
        #     derivative_source_model_ids.update((edge_from_source_node.attribute_recipe_update.switch_model,))
        #     derivative_source_model_ids.update((edge_from_source_node.tail_node.attribute_recipe_update.switch_model,))

        min_time_grain: Optional[TimeGranularity] = None
        if len(min_time_grains) > 0:
            for time_grain in min_time_grains:
                if min_time_grain is None:
                    min_time_grain = time_grain
                elif time_grain is not None and time_grain.to_int() < min_time_grain.to_int():
                    min_time_grain = time_grain

        return AttributeSubgraphResult(
            model_ids=derivative_source_model_ids,
            min_time_grain=min_time_grain,
            subgraph=subgraph,
        )

        # Find the required successor nodes for the group-by-attribute root node
        # aggregation_nodes = semantic_graph.nodes_with_label(AggregationLabel())
        #
        # result = path_finder.find_reachable_targets_simple(
        #     graph=self._semantic_graph,
        #     mutable_path=self._mutable_path,
        #     source_node=source_node,
        #     candidate_target_nodes=aggregation_nodes,
        #     max_path_weight=GroupByAttributeSubgraphGenerator._MAX_SEARCH_DEPTH,
        #     weight_function=BlockMetricDefinitionEdgeWeightFunction(),
        # )
        #
        # found_aggregation_nodes = result.reachable_targets
        # if self._verbose_debug_logs:
        #     logger.debug(
        #         LazyFormat(
        #             "Found target nodes",
        #             source_node=source_node,
        #             found_target_nodes=found_aggregation_nodes,
        #             target_nodes=aggregation_nodes,
        #         )
        #     )
        #
        # if len(found_aggregation_nodes) != 1:
        #     raise RuntimeError(
        #         LazyFormat(
        #             "Did not find exactly one aggregation node. This indicates an error in graph"
        #             " construction.",
        #             source_node=source_node,
        #             found_aggregation_nodes=found_aggregation_nodes,
        #         )
        #     )
        # aggregation_node = mf_first_item(found_aggregation_nodes)

        # subgraph = MutableSemanticGraph.create()
        # subgraph.update(self._generate_attribute_subgraph(aggregation_node))

        # semantic_model_nodes = semantic_graph.nodes_with_label(SemanticModelLabel())
        #
        # result = path_finder.find_reachable_targets_simple(
        #     graph=self._semantic_graph,
        #     mutable_path=self._mutable_path,
        #     source_node=source_node,
        #     candidate_target_nodes=semantic_model_nodes,
        #     max_path_weight=GroupByAttributeSubgraphGenerator._MAX_SEARCH_DEPTH,
        #     weight_function=BlockMetricDefinitionEdgeWeightFunction(),
        # )
        # found_semantic_model_nodes = result.reachable_targets
        # if len(found_aggregation_nodes) != 1:
        #     raise RuntimeError(
        #         LazyFormat(
        #             "Did not find exactly one aggregation node. This indicates an error in graph"
        #             " construction.",
        #             source_node=source_node,
        #             found_semantic_model_nodes=found_semantic_model_nodes,
        #         )
        #     )
        # semantic_model_node = mf_first_item(found_semantic_model_nodes)
        # subgraph.update(self._generate_attribute_subgraph(semantic_model_node))

        # combined_attribute_computation = MutableAttributeComputation()
        # combined_attribute_computation.append_update(aggregation_node.attribute_computation_update)
        # combined_attribute_computation.append_update(semantic_model_node.attribute_computation_update)
        #
        # return AttributeSubgraphResult(
        #     root_attribute_computation=combined_attribute_computation,
        #     subgraph=subgraph,
        # )

    def _generate_attribute_subgraph(
        self,
        source_node: SemanticGraphNode,
    ) -> MutableSemanticGraph:
        path_finder = self._path_finder
        semantic_graph = self._semantic_graph

        result = path_finder.find_descendant_nodes(
            graph=self._semantic_graph,
            mutable_path=self._mutable_path,
            source_node=source_node,
            candidate_target_nodes=semantic_graph.nodes_with_label(GroupByAttributeLabel.get_instance()),
            max_path_weight=GroupByAttributeSubgraphGenerator._MAX_SEARCH_DEPTH,
            weight_function=BlockMetricDefinitionEdgeWeightFunction(),
        )

        nodes_in_path_to_group_by_attribute_nodes = FrozenOrderedSet(
            node for node in result.descendant_nodes if node is not source_node
        )

        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Found nodes in path to attribute nodes",
                    nodes_in_path_to_group_by_attribute_nodes=nodes_in_path_to_group_by_attribute_nodes,
                )
            )

        subgraph = MutableSemanticGraph.create()
        for edge in semantic_graph.adjacent_edges(nodes_in_path_to_group_by_attribute_nodes):
            if (
                edge.head_node in nodes_in_path_to_group_by_attribute_nodes
                and edge.tail_node in nodes_in_path_to_group_by_attribute_nodes
            ):
                subgraph.add_edge(edge)

        root_node = GroupByAttributeRootNode()
        for edge in semantic_graph.edges_with_tail_node(source_node):
            if edge.head_node in nodes_in_path_to_group_by_attribute_nodes:
                subgraph.add_edge(
                    EntityRelationshipEdge.get_instance(
                        tail_node=root_node,
                        head_node=edge.head_node,
                    )
                )
        return subgraph

        # subgraph_edge_candidates = semantic_graph.adjacent_edges(nodes_in_path_to_group_by_attribute_nodes)
        # subgraph_edges = MutableOrderedSet[SemanticGraphEdge]()
        #
        # for edge in subgraph_edge_candidates:
        #     if (
        #         edge.head_node in nodes_in_path_to_group_by_attribute_nodes
        #         and edge.tail_node in nodes_in_path_to_group_by_attribute_nodes
        #     ):
        #         if edge.tail_node is source_node:
        #             subgraph_edges.add(
        #                 EntityRelationshipEdge.get_instance(
        #                     tail_node=root_node,
        #                     head_node=edge.head_node,
        #                 )
        #             )
        #         subgraph_edges.add(edge)
        #
        # subgraph = MutableSemanticGraph.create()
        # subgraph.add_edges(subgraph_edges)
        # return subgraph

    def _replace_join_to_node_with_group_by_attribute_root_node(
        self, join_from_node: SemanticGraphNode, edges: Iterable[SemanticGraphEdge]
    ) -> MutableOrderedSet[SemanticGraphEdge]:
        updated_edges = MutableOrderedSet[SemanticGraphEdge]()
        root_node = GroupByAttributeRootNode()
        for edge in edges:
            if edge.tail_node is join_from_node:
                updated_edges.add(
                    EntityRelationshipEdge.get_instance(
                        tail_node=root_node,
                        head_node=edge.head_node,
                    )
                )
            else:
                updated_edges.add(edge)
        return updated_edges


@dataclass
class AttributeSubgraphResult:
    model_ids: MutableOrderedSet[SemanticModelId]
    min_time_grain: Optional[TimeGranularity]
    subgraph: MutableSemanticGraph


class BlockMetricDefinitionEdgeWeightFunction(
    WeightFunction[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath]
):
    """Weight function that blocks traversal using metric definition edges.

    This reduces the search space for finding the group-by attribute nodes reachable from the metric node and prevents
    incorrect results.
    """

    def __init__(self) -> None:
        self._blocked_label = MetricDefinitionLabel.get_instance()

    @override
    def incremental_weight(
        self, path_to_node: AttributeRecipeWriterPath, next_edge: SemanticGraphEdge, max_path_weight: int
    ) -> Optional[int]:
        if self._blocked_label in next_edge.labels:
            return None
        return 1


@dataclass
class FindReplacementTargetNodesResult:
    aggregation_nodes: MutableOrderedSet[SemanticGraphNode]
