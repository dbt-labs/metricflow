# from collections import defaultdict
# from dataclasses import dataclass
# from typing import FrozenSet, Tuple, Dict, List, Sequence
#
# from metricflow_semantics.experimental.semantic_graph_old.builder.in_progress_semantic_graph import InProgressSemanticGraph
# from metricflow_semantics.experimental.semantic_graph_old.graph_edges import SemanticGraphEdge, \
#     SemanticGraphPathStatOperation, SemanticGraphPathStat
# from metricflow_semantics.experimental.semantic_graph_old.graph_nodes import SemanticGraphNode, AttributeNode
#
#
# @dataclass(frozen=True)
# class AnyEndNodeSet:
#     candidate_node: FrozenSet[SemanticGraphNode]
#
#
# @dataclass(frozen=True)
# class AllEndNodeSet:
#     candidate_nodes: FrozenSet[SemanticGraphNode]
#
#
# @dataclass(frozen=True)
# class PathSegment:
#     node_sequence: Tuple[SemanticGraphNode, ...]
#
#
# @dataclass(frozen=True)
# class SemanticGraphPathRequirement:
#     start_nodes: FrozenSet[SemanticGraphNode]
#     required_path_segments: FrozenSet[PathSegment]
#     max_join_hop_count: int
#     max_entity_link_count: int
#
#
# @dataclass
# class PathPointer:
#     start_node: SemanticGraphNode
#     previous_node: SemanticGraphNode
#     path_stat: SemanticGraphPathStat
#
#
# @dataclass(frozen=True)
# class PathPointerKey:
#     current_node: SemanticGraphNode
#     start_node: SemanticGraphNode
#
#
# class NodeConnectivityTracker:
#
#     def __init__(self, semantic_graph_old: InProgressSemanticGraph, path_requirement: SemanticGraphPathRequirement) -> None:
#         self._semantic_graph = semantic_graph_old
#         self._path_pointers: Dict[SemanticGraphNode, List[PathPointer]] = defaultdict(list)
#         self._path_requirement = path_requirement
#
#     def add_edge(self, edge: SemanticGraphEdge) -> None:
#         start_node_to_path_pointers_at_head_node = {}
#         for path_pointer_at_head_node in self._path_pointers[edge.head_node]:
#             start_node_to_path_pointers_at_head_node[path_pointer_at_head_node.start_node] = path_pointer_at_head_node
#
#         new_path_pointers_for_head_node = []
#
#         for path_pointer_at_tail_node in self._path_pointers[edge.tail_node]:
#             new_path_pointer = PathPointer(
#                 start_node=path_pointer_at_tail_node.start_node,
#                 previous_node=edge.tail_node,
#                 path_stat=edge.get_updated_path_stat(path_pointer_at_tail_node.path_stat),
#             )
#
#             if (
#                 new_path_pointer.path_stat.join_hop_count < self._path_requirement.max_join_hop_count
#                 and new_path_pointer.path_stat.entity_link_count < self._path_requirement.max_entity_link_count
#             ):
#                 new_path_pointers_for_head_node.append(new_path_pointer)
#
#         self._path_pointers[edge.head_node].extend(new_path_pointers_for_head_node)
#         return
#
#     def get_path_pointers(self, node: SemanticGraphNode) -> Sequence[PathPointer]:
#         return self._path_pointers[node]
