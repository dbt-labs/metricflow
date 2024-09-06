# from __future__ import annotations
#
# from abc import ABC, abstractmethod
# from dataclasses import dataclass
# from typing import Sequence, Tuple
#
# from typing_extensions import override
#
# from metricflow_semantics.experimental.semantics.semantic_graph import (
#     SemanticGraph,
#     SemanticGraphNode,
# )
# from metricflow_semantics.experimental.semantics.semantic_model_path import (
#     SemanticGraphPath,
# )
#
#
# class PathTerminationCondition(ABC):
#     @abstractmethod
#     def should_terminate_path(self, path: SemanticGraphPath) -> bool:
#         raise NotImplementedError
#
#
# class ExceedsMaximumEntityHopsCondition(PathTerminationCondition):
#     def __init__(self, max_hops: int) -> None:  # noqa: D
#         self._max_hops = max_hops
#
#     @override
#     def should_terminate_path(self, path: SemanticGraphPath) -> bool:
#         if len(path.edges) <= self._max_hops:
#             return False
#
#         return all(node.as_union.entity_node is not None for node in path.last_nodes(self._max_hops))
#
#
# class MatchesTrailingNodesCondition(PathTerminationCondition):
#     def __init__(self, trailing_nodes: Sequence[SemanticGraphNode]) -> None:  # noqa: D
#         self._trailing_nodes = tuple(trailing_nodes)
#
#     @override
#     def should_terminate_path(self, path: SemanticGraphPath) -> bool:
#         return tuple(path.last_nodes(len(self._trailing_nodes))) == self._trailing_nodes
#
#
# @dataclass(frozen=True)
# class TerminatedPathSet:
#     termination_condition: PathTerminationCondition
#     paths: Tuple[SemanticGraphPath, ...]
#
#
# @dataclass(frozen=True)
# class PathFindingResult:
#     terminated_path_sets: Tuple[TerminatedPathSet, ...]
#
#
# class SemanticGraphPathFinder:
#     def __init__(self, semantic_graph: SemanticGraph) -> None:
#         self._semantic_graph = semantic_graph
#
#     def find_paths(
#         self, start_node: SemanticGraphNode, termination_conditions: Sequence[PathTerminationCondition]
#     ) -> PathFindingResult:
#         raise NotImplementedError
