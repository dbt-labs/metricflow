from __future__ import annotations

import logging
from typing import Iterable

from metricflow_semantics.experimental.metricflow_exception import MetricflowAssertionError
from metricflow_semantics.experimental.ordered_set import MutableOrderedSet
from metricflow_semantics.experimental.semantic_graph.edges.entity_relationship import (
    EntityRelationship,
    EntityRelationshipEdge,
)
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import MeasureNode
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import GroupByAttributeRootNode
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    GroupByAttributeLabel,
    JoinFromLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.graph_path import MutableMetricflowGraphPath
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

    def generate_subgraph_for_one_measure(self, measure_attribute_node: MeasureNode) -> MutableSemanticGraph:
        path_finder = self._path_finder
        semantic_graph = self._semantic_graph
        # group_by_attribute_nodes = semantic_graph.nodes_with_label(GroupByAttributeLabel()).union(
        #     semantic_graph.nodes_with_label(TimeDimensionLabel())
        # )

        label = JoinFromLabel()
        join_from_nodes = MutableOrderedSet[SemanticGraphNode](semantic_graph.nodes_with_label(label))
        find_nearest_join_from_node_result = path_finder.find_descendant_nodes(
            source_node=measure_attribute_node,
            candidate_target_nodes=join_from_nodes,
            max_path_weight=0,
            weight_function=DefaultWeightFunction(),
            mutable_path=MutableMetricflowGraphPath.create(),
        )
        found_target_nodes = tuple(find_nearest_join_from_node_result.found_target_nodes)
        logger.debug(
            LazyFormat(
                "Found nearest nodes with label",
                label=label,
                candidate_target_nodes=join_from_nodes,
                found_target_nodes=found_target_nodes,
            )
        )

        if len(found_target_nodes) != 1:
            raise MetricflowAssertionError(
                LazyFormat(
                    "Expected to find exactly one a join-from node as a descendant of the measure node. "
                    "This might be due to incorrect graph construction.",
                    measure_attribute_node=measure_attribute_node,
                    join_from_nodes=join_from_nodes,
                )
            )
        join_from_node = found_target_nodes[0]

        nodes_in_path_to_group_by_attribute_nodes = path_finder.find_descendant_nodes(
            source_node=join_from_node,
            candidate_target_nodes=semantic_graph.nodes_with_label(GroupByAttributeLabel()),
            max_path_weight=3,
            weight_function=DefaultWeightFunction(),
            mutable_path=MutableMetricflowGraphPath.create(),
        ).descendant_nodes
        nodes_in_path_to_group_by_attribute_nodes.add(join_from_node)

        logger.debug(
            LazyFormat(
                "Found nodes in path to attribute nodes",
                nodes_in_path_to_group_by_attribute_nodes=nodes_in_path_to_group_by_attribute_nodes,
            )
        )

        subgraph_edges = semantic_graph.adjacent_edges(nodes_in_path_to_group_by_attribute_nodes)
        subgraph_edges = self._replace_join_to_node_with_group_by_attribute_root_node(
            join_from_node=join_from_node,
            edges=subgraph_edges,
        )
        subgraph = MutableSemanticGraph.create()
        subgraph.add_edges(subgraph_edges)
        return subgraph
        # for matching_descendant in target_nodes_result.matching_descendants:
        #     result_graph.add_edge(
        #         EntityRelationshipEdge.get_instance(
        #             tail_node=sentinel_node,
        #             relationship=EntityRelationship.VALID,
        #             head_node=matching_descendant,
        #             weight=0,
        #         )
        #     )

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
                        relationship=EntityRelationship.VALID,
                        head_node=edge.head_node,
                    )
                )
            else:
                updated_edges.add(edge)
        return updated_edges
