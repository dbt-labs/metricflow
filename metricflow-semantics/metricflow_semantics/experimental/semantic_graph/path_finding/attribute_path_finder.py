# from __future__ import annotations
#
# import logging
# from collections.abc import Sequence
#
# from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
# from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, OrderedSet
# from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import AttributeNode
# from metricflow_semantics.experimental.semantic_graph.nodes.node_label import DsiEntityLabel
# from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
#     SemanticGraphEdge,
#     SemanticGraphNode,
# )
# from metricflow_semantics.experimental.semantic_graph.path_finding.networkx_path_finder import NetworkxPathFinder
# from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder import (
#     DefaultWeightFunction,
#     EdgeWeightFunction,
#     MetricflowGraphPath,
# )
# from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph, SemanticGraph
#
# logger = logging.getLogger(__name__)
#
# SemanticGraphPath = MetricflowGraphPath[SemanticGraphNode, SemanticGraphEdge]
#
#
# class AttributePathFinder:
#     def __init__(
#         self,
#         semantic_graph: SemanticGraph,
#     ) -> None:
#         self._semantic_graph = semantic_graph
#         self._semantic_graph_path_finder = NetworkxPathFinder(semantic_graph)
#         dsi_entity_subgraph = MutableSemanticGraph.create()
#         for edge in semantic_graph.edges_with_label(DsiEntityLabel()):
#             dsi_entity_subgraph.add_edge(edge)
#         self._dsi_entity_subgraph = dsi_entity_subgraph
#         self._dsi_entity_subgraph_path_finder = NetworkxPathFinder(dsi_entity_subgraph)
#
#     def find_subgraph_from_attribute(self, attribute_node: AttributeNode) -> SemanticGraph:
#         raise NotImplementedError()
#
#     def find_paths_from_attribute(self, attribute_node: AttributeNode) -> Sequence[SemanticGraphPath]:
#         raise NotImplementedError()
#
#     def find_paths_from_attribute_in_subgraph(
#         self, attribute_node: AttributeNode, subgraph: SemanticGraph
#     ) -> Sequence[SemanticGraphPath]:
#         raise NotImplementedError()
#
#     def _find_nearest_dsi_entity_nodes(self, attribute_node: AttributeNode) -> OrderedSet[SemanticGraphNode]:
#         raise NotImplementedError()
#
#     def _find_descendant_subgraph(
#         self, nodes: OrderedSet[SemanticGraphNode], graph: SemanticGraph
#     ) -> Sequence[SemanticGraphPath]:
#         raise NotImplementedError()
#
#     def find_reachable_attribute_subgraph(
#         self,
#         source_attribute_node: AttributeNode,
#     ) -> ReachableAttributeSubgraph:
#         nearest_dsi_entity_nodes = self._semantic_graph_path_finder.find_reachable_descendants(
#             source_node=source_attribute_node,
#             candidate_target_nodes=self._semantic_graph.nodes_with_label(DsiEntityLabel()),
#             weight_function=DefaultWeightFunction(self._semantic_graph),
#             max_path_weight=1,
#         )
#         return ReachableAttributeSubgraph(
#             source_attribute_nodes=FrozenOrderedSet((source_attribute_node,)),
#             source_dsi_entity_nodes=nearest_dsi_entity_nodes,
#             subgraph=self._semantic_graph,
#         )
#
#     def find_all_paths_to_attribute_nodes(
#         self, reachable_attribute_subgraph: ReachableAttributeSubgraph
#     ) -> Sequence[SemanticGraphPath]:
#         all_paths_to_other_dsi_entity_nodes = self._dsi_entity_subgraph_path_finder.find_all_paths(
#             source_nodes=reachable_attribute_subgraph.source_dsi_entity_nodes,
#             target_nodes=self._dsi_entity_subgraph.nodes,
#             cutoff=1,
#         )
#
#         last_nodes_in_dsi_entity_path = FrozenOrderedSet(path.nodes[-1] for path in all_paths_to_other_dsi_entity_nodes)
#
#         shortest_paths_to_other_nodes = self._semantic_graph_path_finder.find_shortest_paths(
#             source_nodes=last_nodes_in_dsi_entity_path,
#             target_nodes=None,
#             weight_function=EdgeWeightFunction(self._semantic_graph),
#             max_path_weight=0,
#         )
#
#         for path in all_paths_to_other_dsi_entity_nodes:
#             raise NotImplementedError()
#
#
# @fast_frozen_dataclass()
# class ReachableAttributeSubgraph:
#     source_attribute_nodes: FrozenOrderedSet[SemanticGraphNode]
#     source_dsi_entity_nodes: FrozenOrderedSet[SemanticGraphNode]
#     subgraph: SemanticGraph
