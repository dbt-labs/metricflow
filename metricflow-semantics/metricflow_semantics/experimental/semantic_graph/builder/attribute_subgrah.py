from __future__ import annotations

import logging

from metricflow_semantics.experimental.ordered_set import MutableOrderedSet
from metricflow_semantics.experimental.semantic_graph.edges.entity_relationship import (
    EntityRelationship,
    EntityRelationshipEdge,
)
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import MeasureAttributeNode
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import GroupByAttributeRootNode
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    DsiEntityLabel,
    GroupByAttributeLabel,
    MetricTimeLabel, TimeDimensionLabel, JoinFromLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder import (
    DefaultWeightFunction,
    MetricflowGraphPathFinder,
)
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph, SemanticGraph
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


class GroupByAttributeSubgraphGenerator:
    def __init__(
        self,
        semantic_graph: SemanticGraph,
        path_finder: MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge],
    ) -> None:
        self._semantic_graph = semantic_graph
        self._path_finder = path_finder

    def generate_subgraph_for_one_measure(self, measure_attribute_node: MeasureAttributeNode) -> MutableSemanticGraph:
        path_finder = self._path_finder
        semantic_graph = self._semantic_graph
        # group_by_attribute_nodes = semantic_graph.nodes_with_label(GroupByAttributeLabel()).union(
        #     semantic_graph.nodes_with_label(TimeDimensionLabel())
        # )

        target_nodes = MutableOrderedSet[SemanticGraphNode]()
        # target_nodes.update(semantic_graph.nodes_with_label(GroupByAttributeLabel()))
        # target_nodes.update(semantic_graph.nodes_with_label(MetricTimeLabel()))
        # target_nodes.update(semantic_graph.nodes_with_label(DsiEntityLabel()))
        target_nodes.update(semantic_graph.nodes_with_label(JoinFromLabel()))
        target_nodes_result = path_finder.find_reachable_descendants(
            source_node=measure_attribute_node,
            candidate_target_nodes=target_nodes,
            max_path_weight=0,
            weight_function=DefaultWeightFunction(graph=semantic_graph),
        )

        logger.debug(LazyFormat(
            "Got target nodes result",
            target_nodes=target_nodes,
            target_nodes_result=target_nodes_result))

        result_graph = MutableSemanticGraph.create()
        sentinel_node = GroupByAttributeRootNode()
        for matching_descendant in target_nodes_result.matching_descendants:
            result = path_finder.find_reachable_descendants(
                source_node=matching_descendant,
                candidate_target_nodes=semantic_graph.nodes_with_label(GroupByAttributeLabel()),
                max_path_weight=3,
                weight_function=DefaultWeightFunction(graph=semantic_graph),
            )
            logger.info(LazyFormat("Got descendants", source_node=matching_descendant, edges=result.required_edges))
            for edge in result.required_edges:
                if edge.tail_node is matching_descendant:
                    result_graph.add_edge(
                        EntityRelationshipEdge.get_instance(
                            tail_node=sentinel_node,
                            relationship=EntityRelationship.VALID,
                            head_node=edge.head_node,
                            weight=0,
                        )
                    )
                else:
                    result_graph.add_edge(edge)


        # for matching_descendant in target_nodes_result.matching_descendants:
        #     result_graph.add_edge(
        #         EntityRelationshipEdge.get_instance(
        #             tail_node=sentinel_node,
        #             relationship=EntityRelationship.VALID,
        #             head_node=matching_descendant,
        #             weight=0,
        #         )
        #     )

        return result_graph
