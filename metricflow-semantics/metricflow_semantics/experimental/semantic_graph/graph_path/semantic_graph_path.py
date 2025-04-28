# from __future__ import annotations
#
# import logging
# from typing import FrozenSet, Optional, Sequence, Set, Tuple, override
#
# from metricflow_semantics.experimental.semantic_graph.graph_edges import SemanticGraphEdge, SemanticGraphEdgeType
# from metricflow_semantics.experimental.semantic_graph.graph_nodes import AttributeNode, SemanticGraphNode
# from metricflow_semantics.experimental.semantic_graph.graph_path.measure_attribute_computation import (
#     MeasureAttributeComputation,
# )
# from metricflow_semantics.experimental.semantic_graph.semantic_graph import SemanticGraph
# from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
# from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
# from metricflow_semantics.mf_logging.pretty_print import mf_pformat
#
# logger = logging.getLogger(__name__)
#
# # @dataclass(frozen=True)
# # class TailToHeadSemanticGraphPath:
# #     tail_attribute_node: AttributeNode
# #     edges: Tuple[SemanticGraphEdge, ...] = dataclasses.field(default_factory=list)
# #
# #
# # @dataclass(frozen=True)
# # class HeadToTailSemanticGraphPath:
# #     tail_attribute_node: AttributeNode
# #     edges: List[SemanticGraphEdge]
# #     properties: SemanticGraphPathPropertySet
#
# #
# # class SemanticGraphPathPropertyRule(ABC):
# #
# #     @abstractmethod
# #     def generate_properties(
# #         self,
# #         previous_property: SemanticGraphPathPropertySet,
# #         edge: SemanticGraphEdge,
# #     ) -> SemanticGraphPathPropertySet:
# #         raise NotImplementedError
#
#
# class SemanticGraphPath(MetricFlowPrettyFormattable):
#     def __init__(
#         self,
#         start_node: SemanticGraphNode,
#         end_node: SemanticGraphNode,
#         edges: Tuple[SemanticGraphEdge, ...],
#         computation: MeasureAttributeComputation,
#     ) -> None:
#         if len(edges) == 0:
#             assert end_node == start_node
#
#         self._start_node = start_node
#         self._end_node = end_node
#         self._edges = edges
#         self._computation = computation
#         node_set = {start_node}
#         if len(edges) > 0:
#             for edge in self.edges:
#                 node_set.add(edge.tail_node)
#         self._node_set = frozenset(node_set)
#
#     @property
#     def start_node(self) -> SemanticGraphNode:
#         return self._start_node
#
#     @property
#     def end_node(self) -> SemanticGraphNode:
#         return self._end_node
#
#     @property
#     def edges(self) -> Sequence[SemanticGraphEdge]:
#         return self._edges
#
#     @property
#     def computation(self) -> MeasureAttributeComputation:
#         return self._computation
#
#     @staticmethod
#     def create_initial_path(initial_node: SemanticGraphNode) -> SemanticGraphPath:
#         return SemanticGraphPath(
#             start_node=initial_node,
#             end_node=initial_node,
#             edges=(),
#             computation=MeasureAttributeComputation(),
#         )
#
#     def add_edge_to_start(self, edge: SemanticGraphEdge) -> SemanticGraphPath:
#         if len(self._edges) == 0:
#             assert edge.head_node == self._start_node
#
#             return SemanticGraphPath(
#                 start_node=edge.tail_node,
#                 end_node=self.end_node,
#                 edges=(edge,),
#                 computation=edge.computation_method.update_computation(
#                     tail_node=edge.tail_node,
#                     head_node=edge.head_node,
#                     computation=self.computation,
#                 ),
#             )
#
#         assert edge.head_node == self.start_node
#         return SemanticGraphPath(
#             start_node=edge.tail_node,
#             end_node=self.end_node,
#             edges=(edge,) + self.edges,
#             computation=edge.computation_method.update_computation(
#                 tail_node=edge.tail_node,
#                 head_node=edge.head_node,
#                 computation=self.computation,
#             ),
#         )
#
#     def add_edge_to_end(self, edge: SemanticGraphEdge) -> SemanticGraphPath:
#         if len(self._edges) == 0:
#             assert edge.tail_node == self._start_node
#
#             return SemanticGraphPath(
#                 start_node=self.start_node,
#                 end_node=edge.head_node,
#                 edges=(edge,),
#                 computation=edge.computation_method.update_computation(
#                     tail_node=edge.tail_node,
#                     head_node=edge.head_node,
#                     computation=self.computation,
#                 ),
#             )
#
#         assert edge.tail_node == self.end_node
#         return SemanticGraphPath(
#             start_node=self.start_node,
#             end_node=edge.head_node,
#             edges=tuple(self.edges) + (edge,),
#             computation=edge.computation_method.update_computation(
#                 tail_node=edge.tail_node,
#                 head_node=edge.head_node,
#                 computation=self.computation,
#             ),
#         )
#
#     def node_set(self) -> FrozenSet[SemanticGraphNode]:
#         return self._node_set
#
#     def contains_node(self, node: SemanticGraphNode) -> bool:
#         return node in self.node_set()
#
#     @property
#     @override
#     def pretty_format(self) -> Optional[str]:
#         if len(self._edges) == 0:
#             return mf_pformat([self.start_node])
#         nodes = [self.start_node]
#         for edge in self._edges:
#             nodes.append(edge.head_node)
#         return mf_pformat(nodes)
#
#     def join(self, other_path: SemanticGraphPath) -> SemanticGraphPath:
#         for edge in reversed(self.edges):
#             other_path = other_path.add_edge_to_start(edge)
#         return other_path
#
#
# #
# # class SemanticGraphPathFilter(ABC):
# #     def include_path(self, path: Seman):
#
#
# class SemanticGraphPathFinder:
#     def __init__(self, semantic_graph: SemanticGraph) -> None:
#         self._semantic_graph = semantic_graph
#         self._paths_created = 0
#
#     def _increment_paths_created(self) -> None:
#         self._paths_created += 1
#
#     def _extend_path_at_start(
#         self,
#         path: SemanticGraphPath,
#         allowed_edge_types: Set[SemanticGraphEdgeType],
#     ) -> Sequence[SemanticGraphPath]:
#         paths = []
#         for edge in self._semantic_graph.get_edges_to_node(path.start_node):
#             if edge.edge_type not in allowed_edge_types or edge.tail_node in path.node_set():
#                 continue
#
#             paths.append(path.add_edge_to_start(edge))
#             self._increment_paths_created()
#         return paths
#
#     def _extend_path_at_end(
#         self,
#         path: SemanticGraphPath,
#         allowed_edge_types: Set[SemanticGraphEdgeType],
#     ) -> Sequence[SemanticGraphPath]:
#         paths = []
#         for edge in self._semantic_graph.get_edges_from_node(path.end_node):
#             if edge.edge_type not in allowed_edge_types or edge.head_node in path.node_set():
#                 continue
#
#             paths.append(path.add_edge_to_end(edge))
#             self._increment_paths_created()
#         return paths
#
#     def _search_for_paths_from_end_node(
#         self,
#         start_node: SemanticGraphNode,
#         end_node: SemanticGraphNode,
#         allowed_edge_types: Set[SemanticGraphEdgeType],
#     ) -> Sequence[SemanticGraphPath]:
#         matching_paths = []
#         current_paths = [SemanticGraphPath.create_initial_path(end_node)]
#         while True:
#             next_current_paths = []
#             for current_path in current_paths:
#                 next_possible_paths = self._extend_path_at_start(current_path, allowed_edge_types)
#
#                 if len(next_possible_paths) == 0:
#                     continue
#
#                 for next_possible_path in next_possible_paths:
#                     if next_possible_path.start_node == start_node:
#                         matching_paths.append(next_possible_path)
#                     else:
#                         next_current_paths.append(next_possible_path)
#
#             if len(next_current_paths) == 0:
#                 break
#
#             current_paths = next_current_paths
#
#         return matching_paths
#
#     def find_possible_paths(
#         self,
#         start_node: AttributeNode,
#         end_node: AttributeNode,
#         allowed_edge_types: Set[SemanticGraphEdgeType],
#     ) -> Sequence[SemanticGraphPath]:
#         self._paths_created = 0
#         matching_paths = []
#
#         path_from_start = SemanticGraphPath.create_initial_path(start_node)
#         while True:
#             next_possible_paths = self._extend_path_at_end(
#                 path_from_start,
#                 allowed_edge_types,
#             )
#             if len(next_possible_paths) != 1:
#                 break
#             path_from_start = next_possible_paths[0]
#
#         new_start_node = path_from_start.end_node
#
#         logger.info(LazyFormat("Searing for paths.", path_from_start=path_from_start, end_node=end_node))
#
#         paths_from_new_start_node_to_end_node = self._search_for_paths_from_end_node(
#             start_node=new_start_node,
#             end_node=end_node,
#             allowed_edge_types=allowed_edge_types,
#         )
#
#         matching_paths = [path_from_start.join(path) for path in paths_from_new_start_node_to_end_node]
#
#         logger.info(
#             LazyFormat(
#                 "Finished finding paths.",
#                 matching_path_count=len(matching_paths),
#                 paths_created=self._paths_created,
#             )
#         )
#         return matching_paths
