# from __future__ import annotations
#
# import logging
# import typing
# from typing import Any, Generic, Optional, Sequence
#
# import networkx as nx
# from typing_extensions import override
#
# from metricflow_semantics.collection_helpers.mf_type_aliases import Pair
# from metricflow_semantics.experimental.mf_graph.mf_graph import EdgeT, MetricflowGraph, NodeT
# from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
# from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder import (
#     EdgeWeightFunction,
#     MetricflowGraphPath,
#     MetricflowGraphPathFinder,
#     MutableMetricflowGraphPath,
# )
# from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
#
# if typing.TYPE_CHECKING:
#     from _typeshed import SupportsGetItem
#
#
# logger = logging.getLogger(__name__)
#
#
# class NetworkxPathFinder(MetricflowGraphPathFinder[NodeT, EdgeT]):
#     def __init__(self, graph: MetricflowGraph[NodeT, EdgeT]) -> None:
#         super().__init__(graph)
#         self._initialized_nx_digraph: Optional[nx.DiGraph] = None
#         self._initialized_node_pair_to_edge: Optional[dict[Pair[NodeT, NodeT], EdgeT]] = None
#
#     @property
#     def _nx_graph(self) -> nx.DiGraph:
#         if self._initialized_nx_digraph is not None:
#             return self._initialized_nx_digraph
#
#         graph: nx.DiGraph[NodeT] = nx.DiGraph()
#         for node in self._mf_graph.nodes:
#             graph.add_node(node)
#         for edge in self._mf_graph.edges:
#             graph.add_edge(
#                 edge.tail_node, edge.head_node, **{NetworkxEdgeWeightFunction.NX_EDGE_ATTRIBUTES__EDGE_KEY: edge}
#             )
#         self._initialized_nx_digraph = graph
#         return graph
#
#     @property
#     def _node_pair_to_edge(self) -> dict[Pair[NodeT, NodeT], EdgeT]:
#         if self._initialized_node_pair_to_edge is not None:
#             return self._initialized_node_pair_to_edge
#
#         self._initialized_node_pair_to_edge = {edge.node_pair: edge for edge in self._mf_graph.edges}
#         return self._initialized_node_pair_to_edge
#
#     def find_reachable_descendants(
#         self,
#         source_node: NodeT,
#         candidate_target_nodes: OrderedSet[NodeT],
#         weight_function: EdgeWeightFunction[NodeT, EdgeT],
#         max_path_weight: int,
#     ) -> FrozenOrderedSet[NodeT]:
#         matching_target_nodes = self._find_descendants_for_one_node(
#             source_node=source_node,
#             candidate_target_nodes=candidate_target_nodes,
#             max_path_weight=max_path_weight,
#             weight_function=NetworkxEdgeWeightFunction(weight_function),
#         )
#
#         return matching_target_nodes.as_frozen()
#
#     def _find_descendants_for_one_node(
#         self,
#         source_node: NodeT,
#         candidate_target_nodes: OrderedSet[NodeT],
#         weight_function: NetworkxEdgeWeightFunction[NodeT, EdgeT],
#         max_path_weight: int,
#     ) -> OrderedSet[NodeT]:
#         node_to_shortest_path_length = nx.single_source_dijkstra_path_length(
#             G=self._nx_graph,
#             source=source_node,
#             cutoff=max_path_weight,
#             weight=weight_function.nx_weight_arg,
#         )
#
#         matching_candidate_nodes = MutableOrderedSet[NodeT]()
#
#         for node, node_to_shortest_path_length in node_to_shortest_path_length.items():
#             if node in candidate_target_nodes:
#                 matching_candidate_nodes.add(node)
#         logger.debug(
#             LazyFormat(
#                 "Found descendants for one node",
#                 source_node=source_node,
#                 candidate_target_nodes=candidate_target_nodes,
#                 node_to_shortest_path_length=node_to_shortest_path_length,
#             )
#         )
#         return candidate_target_nodes
#
#     @override
#     def find_all_paths(
#         self,
#         source_nodes: OrderedSet[NodeT],
#         target_nodes: OrderedSet[NodeT],
#         cutoff: int,
#     ) -> Sequence[MetricflowGraphPath[NodeT, EdgeT]]:
#         # TODO: Use DFS to make this more efficient for a set of source nodes.
#         paths = []
#         for source_node in source_nodes:
#             for target_node in target_nodes:
#                 for path in nx.all_simple_edge_paths(
#                     G=self._nx_graph,
#                     source=source_node,
#                     target=target_node,
#                     cutoff=cutoff,
#                 ):
#                     paths.append(
#                         MutableMetricflowGraphPath.create(self._node_pair_to_edge[node_pair] for node_pair in path)
#                     )
#         return paths
#
#     @override
#     def find_shortest_paths(
#         self,
#         source_nodes: OrderedSet[NodeT],
#         target_nodes: Optional[OrderedSet[NodeT]],
#         weight_function: EdgeWeightFunction[NodeT, EdgeT],
#         max_path_weight: int,
#     ) -> Sequence[MetricflowGraphPath[NodeT, EdgeT]]:
#         nx_shortest_paths: list[Pair[NodeT, NodeT]] = []
#
#         if target_nodes is None or len(target_nodes) == 0:
#             source_node_to_length_dict, source_node_to_path_dict = nx.multi_source_dijkstra(
#                 G=self._nx_graph,
#                 sources=source_nodes,
#                 weight=NetworkxEdgeWeightFunction(weight_function).nx_weight_arg,
#                 cutoff=max_path_weight,
#             )
#             nx_shortest_paths.extend(source_node_to_path_dict.values())
#         for target_node in target_nodes:
#             source_node_to_length_dict, source_node_to_path_dict = nx.multi_source_dijkstra(
#                 G=self._nx_graph,
#                 sources=source_nodes,
#                 target=target_node,
#                 weight=NetworkxEdgeWeightFunction(weight_function).nx_weight_arg,
#                 cutoff=max_path_weight,
#             )
#             nx_shortest_paths.extend(source_node_to_path_dict.values())
#
#         shortest_paths: list[MetricflowGraphPath[NodeT, EdgeT]] = []
#         for nx_path in nx_shortest_paths:
#             nx_path_length = len(nx_path)
#             mf_path = MutableMetricflowGraphPath.create()
#             for i in range(nx_path_length):
#                 if i == nx_path_length - 1:
#                     break
#                 mf_path.append(self._node_pair_to_edge[(nx_path[i], nx_path[i + 1])])
#             shortest_paths.append(mf_path)
#         return shortest_paths
#
#
# class NetworkxEdgeWeightFunction(Generic[NodeT, EdgeT]):
#     # `nx` its own representation of an edge.
#     # `nx` stores an attributes dictionary for each edge in its representation.
#     # This is the name of the key to get the MF edge object
#     NX_EDGE_ATTRIBUTES__EDGE_KEY = "mf_edge"
#
#     def __init__(self, edge_weight_function: EdgeWeightFunction[NodeT, EdgeT]) -> None:
#         self._mf_edge_weight_function = edge_weight_function
#
#     def nx_weight_arg(  # type: ignore[misc]
#         self, tail_node: Any, head_node: Any, edge_attribute_mapping: SupportsGetItem[str, Any]
#     ) -> Optional[float]:
#         edge = edge_attribute_mapping[NetworkxEdgeWeightFunction.NX_EDGE_ATTRIBUTES__EDGE_KEY]
#         mf_edge_weight = self._mf_edge_weight_function.weight(edge)
#         return float(mf_edge_weight) if mf_edge_weight is not None else None
