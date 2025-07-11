from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Sequence

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.ordered_set import OrderedSet
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    DsiEntityLabel,
    JoinedModelLabel,
    KeyEntityClusterLabel,
    LocalModelLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import SemanticGraphNode
from metricflow_semantics.experimental.semantic_graph.semantic_graph import SemanticGraph
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


# @dataclass
# class NodePredecessorContext:
#     # If coming from a path that originates from the predecessor node to this node, what is the weight
#     # of that path?
#     predecessor_nodes_by_path_weight_to_this_node: dict[int, set[SemanticGraphNode]] = dataclasses.field(default_factory=defaultdict(set))
#     predecessor_node_to_path_key_query: dict[SemanticGraphNode, AnyLengthTuple[str]] = dataclasses.field(default_factory=dict)
#
#
# class NodeReachabilityLookup:
#     def __init__(self, target_node: SemanticGraphNode) -> None:
#         raise NotImplementedError
#
#     def get_weight_from_predecessor(self, predecessor_node: SemanticGraphNode) -> int:
#         raise NotImplementedError
#
#     def add_weight_from_predecessor(self, predecessor_node: SemanticGraphNode, added_weight: int) -> None:
#         raise NotImplementedError
#
#     def get_min_weight(self) -> int:
#         raise NotImplementedError
#
#
# @dataclass
# class ExploreNodeContext:
#     target_node: SemanticGraphNode
#     node_to_explore: SemanticGraphNode
#     weight_remaining: int

DunderNameTuple = AnyLengthTuple[str]

# (key0, key1) with 1 weight remaining
# (key,) with 2 weight remaining


@dataclass
class NodeReachabilityContext:
    def get_key_queries(self) -> Sequence[DunderNameTuple]:
        pass

    def get_weight_for_key_query(self, key_query: DunderNameTuple):
        pass

    def add_key_query(self, key_query: DunderNameTuple):


class KeyQueryResolver:
    # def __init__(self, semantic_graph: SemanticGraph) -> None:
    #     self._semantic_graph = semantic_graph
    #
    # def generate_entity_key_queries(self) -> AnyLengthTuple[DsiEntityKeyQuery]:
    #     graph = self._semantic_graph
    #     local_model_nodes = graph.nodes_with_label(LocalModelLabel.get_instance())
    #     allowed_nodes = (
    #         local_model_nodes
    #         .union(graph.nodes_with_label(JoinedModelLabel.get_instance()))
    #         .union(graph.nodes_with_label(KeyEntityClusterLabel.get_instance()))
    #         .union(graph.nodes_with_label(DsiEntityLabel.get_instance()))
    #     )
    #     raise NotImplementedError

    def __init__(self) -> None:  # noqa: D
        self._verbose_debug_logs = True

    def find_paths(
        self,
        graph: SemanticGraph,
        source_nodes: OrderedSet[SemanticGraphNode],
        target_nodes: OrderedSet[SemanticGraphNode],
        max_dunder_name_length: int = 2,
    ) -> dict[SemanticGraphNode, AnyLengthTuple[DunderNameTuple]]:
        local_model_nodes = graph.nodes_with_label(LocalModelLabel.get_instance())
        allowed_nodes = (
            local_model_nodes.union(graph.nodes_with_label(JoinedModelLabel.get_instance()))
            .union(graph.nodes_with_label(KeyEntityClusterLabel.get_instance()))
            .union(graph.nodes_with_label(DsiEntityLabel.get_instance()))
        )

        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Starting path-finding for key queries",
                    source_nodes=source_nodes,
                    target_nodes=target_nodes,
                    allowed_nodes=allowed_nodes,
                )
            )

        node_to_reachable_targets: dict[SemanticGraphNode, list[tuple[DunderNameTuple, int]]] = defaultdict(list)

        for target_node in target_nodes:
            dunder_name_element = target_node.attribute_recipe_update.add_dunder_name_element
            if dunder_name_element is None:
                raise RuntimeError(
                    LazyFormat(
                        "Expected a target node to have a dunder-name element",
                        target_node=target_node,
                        recipe_update=target_node.attribute_recipe_update,
                    )
                )
            node_to_reachable_targets[target_node].append(((dunder_name_element,), 1))

        nodes_to_process = list(target_nodes)

        while len(nodes_to_process) > 0:
            current_node = nodes_to_process.pop()

            if current_node in source_nodes:
                continue

            current_node_reachable_targets = node_to_reachable_targets[current_node]
            predecessor_edges = graph.edges_with_head_node(current_node)
            for predecessor_edge in predecessor_edges:
                predecessor_node = predecessor_edge.tail_node

                if predecessor_node not in allowed_nodes:
                    continue

                # See what is the weight added by the path from the predecessor.
                added_weight_from_predecessor = 0
                added_dunder_name_elements: AnyLengthTuple[str] = ()
                added_dunder_name_element_by_predecessor_node = (
                    predecessor_node.attribute_recipe_update.add_dunder_name_element
                )
                if added_dunder_name_element_by_predecessor_node is not None:
                    added_weight_from_predecessor += 1
                    added_dunder_name_elements = (added_dunder_name_element_by_predecessor_node,)
                added_dunder_name_element_by_edge = predecessor_edge.attribute_recipe_update.add_dunder_name_element
                if added_dunder_name_element_by_edge is not None:
                    added_weight_from_predecessor += 1
                    added_dunder_name_elements = added_dunder_name_elements + (added_dunder_name_element_by_edge,)

                predecessor_node_reachable_targets = node_to_reachable_targets[predecessor_node]

                process_predecessor_node = False

                for dunder_name_tuple, weight in current_node_reachable_targets:
                    new_weight = weight + added_weight_from_predecessor
                    if new_weight <= max_dunder_name_length:
                        new_dunder_name_tuple = added_dunder_name_elements + dunder_name_tuple
                        logger.debug(LazyFormat("Created new dunder name", new_dunder_name_tuple=new_dunder_name_tuple, predecessor_node=predecessor_node))
                        predecessor_node_reachable_targets.append(
                            (
                                new_dunder_name_tuple,
                                new_weight,
                            )
                        )
                        process_predecessor_node = True

                if process_predecessor_node:
                    nodes_to_process.append(predecessor_node)

        # (('listing',), 1), (('user',), 1)
        node_to_key_queries: dict[SemanticGraphNode, AnyLengthTuple[DunderNameTuple]] = {}
        for source_node in source_nodes:
            node_to_key_queries[source_node] = tuple(
                key_query for key_query, _ in node_to_reachable_targets[source_node]
            )
            # node_to_key_queries[source_node] = node_to_reachable_targets[source_node]

        return node_to_key_queries

    # def _find_paths(
    #         self,
    #         graph: SemanticGraph,
    #         source_nodes: OrderedSet[SemanticGraphNode],
    #         target_nodes: OrderedSet[SemanticGraphNode],
    #         max_weight: int = 1
    # ) -> None:
    #     node_to_reachability_lookup: dict[SemanticGraphNode, NodeReachabilityLookup] = defaultdict(NodeReachabilityLookup)
    #
    #     nodes_to_process: set[SemanticGraphNode] = set(target_nodes)
    #
    #     explore_node_contexts = [
    #         ExploreNodeContext(target_node=node, node_to_explore=node, weight_remaining=max_weight)
    #         for node in target_nodes
    #     ]
    #     while True:
    #
    #         if len(explore_node_contexts) == 0
    #
    #         if len(nodes_to_process) == 0:
    #             break
    #
    #         current_node = nodes_to_process.pop()
    #         predecessor_edges = graph.edges_with_head_node(current_node)
    #         current_node_reachability_lookup = node_to_reachability_lookup[current_node]
    #         for predecessor_edge in predecessor_edges:
    #             predecessor_node = predecessor_edge.tail_node
    #
    #             # See what is the weight added by the path from the predecessor.
    #             added_weight_from_predecessor = 0
    #             if predecessor_edge.attribute_recipe_update.add_entity_link is not None:
    #                 added_weight_from_predecessor += 1
    #             if current_node.attribute_recipe_update.add_entity_link is not None:
    #                 added_weight_from_predecessor += 1
    #
    #             # If the weight is below the threshold, explore further
    #             if current_node_reachability_lookup.get_min_weight() + added_weight_from_predecessor <= max_weight:
    #                 nodes_to_process.add(predecessor_node)
    #             # See if the predecessor can reach the current node.
    #             current_node_reachability_lookup.add_weight_from_predecessor(
    #                 predecessor_node=predecessor_node, added_weight=added_weight_from_predecessor
    #             )
