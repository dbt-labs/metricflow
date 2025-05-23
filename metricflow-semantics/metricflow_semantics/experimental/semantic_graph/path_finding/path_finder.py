from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Set
from dataclasses import dataclass
from typing import Generator, Generic, Optional, Sequence, TypeVar, override

from metricflow_semantics.experimental.mf_graph.mf_graph import (
    MetricflowGraph,
    MetricflowGraphEdge,
    MetricflowGraphNode,
)
from metricflow_semantics.experimental.ordered_set import MutableOrderedSet
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.mf_logging.pretty_formatter import PrettyFormatContext

logger = logging.getLogger(__name__)


EdgeT = TypeVar("EdgeT", bound=MetricflowGraphEdge)
NodeT = TypeVar("NodeT", bound=MetricflowGraphNode)
EdgeWeightFunctionT = TypeVar("EdgeWeightFunctionT", bound="EdgeWeightFunction")


@dataclass
class ReachableDescendantResult(Generic[NodeT, EdgeT]):
    matching_descendants: MutableOrderedSet[NodeT]
    required_edges: MutableOrderedSet[EdgeT]


class MetricflowGraphPathFinder(Generic[NodeT, EdgeT], ABC):
    def __init__(self, graph: MetricflowGraph[NodeT, EdgeT]) -> None:
        self._mf_graph = graph

        # Variables for `find_all_simple_paths`.
        self._finished_visiting_nodes: MutableOrderedSet[NodeT] = MutableOrderedSet()
        self._current_path: MutableMetricflowGraphPath[NodeT, EdgeT] = MutableMetricflowGraphPath.create(
            start_node=None
        )
        self._current_path_weight = 0
        self._node_visit_contexts: list[NodeVisitContext] = []

    def find_reachable_descendants(
        self,
        source_node: NodeT,
        candidate_target_nodes: Set[NodeT],
        weight_function: EdgeWeightFunction[NodeT, EdgeT],
        max_path_weight: int,
    ) -> ReachableDescendantResult:
        matching_descendants = MutableOrderedSet[NodeT]()
        required_edges = MutableOrderedSet[EdgeT]()

        for path in self.find_all_simple_paths(
            source_node=source_node,
            target_nodes=candidate_target_nodes,
            weight_function=weight_function,
            max_path_weight=max_path_weight,
        ):
            logger.debug(LazyFormat("Got path", path=path))
            matching_descendants.add(path.nodes[-1])
            # TODO: This can be improved as it adds a lot of edges repeatedly.
            required_edges.update(path.edges)

        return ReachableDescendantResult(
            matching_descendants=matching_descendants,
            required_edges=required_edges,
        )

    # @abstractmethod
    # def find_all_paths(
    #     self,
    #     source_nodes: OrderedSet[NodeT],
    #     target_nodes: OrderedSet[NodeT],
    #     max_path_weight: int,
    # ) -> Sequence[MetricflowGraphPath[NodeT, EdgeT]]:
    #     raise NotImplementedError()

    def _pop_node_visit_context(self) -> None:
        self._node_visit_contexts.pop(-1)
        self._current_path.pop_end()

    def find_all_simple_paths(
        self,
        source_node: NodeT,
        target_nodes: Set[NodeT],
        weight_function: EdgeWeightFunction[NodeT, EdgeT],
        max_path_weight: int,
    ) -> Generator[MetricflowGraphPath[NodeT, EdgeT], None, None]:
        # Visit the descendants in DFS, starting from the source node.
        self._finished_visiting_nodes = MutableOrderedSet()
        self._current_path = MutableMetricflowGraphPath.create(
            start_node=source_node,
        )
        self._node_visit_contexts = [
            NodeVisitContext(
                node=source_node,
                weight_added_by_edge_to_this_node=0,
                edges_to_process_from_this_node=list(self._mf_graph.edges_with_tail_node(source_node)),
            )
        ]

        while True:
            # Stop if all paths have been evaluated.
            if len(self._node_visit_contexts) == 0:
                return

            # In each iteration of this loop, the current node is the node specified by the last
            # visit context. Check if we can go to the next edge specified in the context.
            # If we can, then append the context for visiting the next node to the end of the context list
            # to get DFS behavior.
            # If we can't, then pop the visit context. Popping the vist context is going back up to the predecessor node,
            # so we have to update the current path appropriately.

            current_node_visit_context = self._node_visit_contexts[-1]
            current_node = current_node_visit_context.node

            logger.debug(LazyFormat("Start visit", current_node=current_node, current_node_visit_context=current_node_visit_context))

            # If we've hit one of the target nodes, so return the path to the node and stop visiting
            # descendants of the current node.
            if current_node in target_nodes:
                logger.debug(LazyFormat("Reached target node, so returning current path", current_node=current_node))
                yield self._current_path
                self._pop_node_visit_context()
                continue

            # If the current node has no descendants, then go to the next visit context.
            edges_to_process_in_current_node = current_node_visit_context.edges_to_process_from_this_node
            if len(edges_to_process_in_current_node) == 0:
                logger.debug(LazyFormat("No more edges remaining for current context, so popping it off."))

                self._finished_visiting_nodes.add(current_node)
                self._pop_node_visit_context()
                continue

            # See if we can go from the current node to the next descendant node.
            next_edge_to_take = edges_to_process_in_current_node.pop(-1)
            next_node = next_edge_to_take.head_node

            # If we can't go to the next node, then restart the loop so that we can check the next edge.
            if next_node in self._finished_visiting_nodes:
                logger.debug(LazyFormat("Skipping node as it has already been visited.", next_node=next_node))
                continue

            # Avoid cycles
            if next_node in self._current_path.node_set:
                logger.debug(LazyFormat("Skipping node as would produce a cycle.", next_node=next_node))
                continue

            next_edge_weight = weight_function.weight(next_edge_to_take)

            if next_edge_weight is None:
                logger.debug(LazyFormat("Skipping edge as the weight is not set", next_edge_to_take=next_edge_to_take))
                continue

            if next_edge_weight + self._current_path.weight > max_path_weight:
                logger.debug(
                    LazyFormat(
                        "Skipping edge as the weight would exceed cutoff",
                        next_edge_to_take=next_edge_to_take,
                        next_edge_weight=next_edge_weight,
                    )
                )
                continue

            # Take the next edge.
            self._node_visit_contexts.append(
                NodeVisitContext(
                    node=next_edge_to_take.head_node,
                    weight_added_by_edge_to_this_node=next_edge_weight,
                    edges_to_process_from_this_node=list(
                        self._mf_graph.edges_with_tail_node(next_edge_to_take.head_node),
                    ),
                )
            )
            self._current_path.append(next_edge_to_take, next_edge_weight)

        return

    # def find_all_descendants(
    #     self,
    #     source_nodes: OrderedSet[NodeT],
    # ) -> FrozenOrderedSet[NodeT]:
    #     all_descendants = MutableOrderedSet[NodeT]()
    #     nodes_to_process: list[NodeT] = list(source_nodes)
    #
    #     while True:
    #         if len(nodes_to_process) == 0:
    #             break
    #         node_to_process = nodes_to_process.pop(0)
    #         all_descendants.add(node_to_process)
    #         for next_edge in self._mf_graph.edges_with_tail_node(node_to_process):
    #             next_head_node = next_edge.head_node
    #             if next_head_node not in all_descendants:
    #                 nodes_to_process.append(next_head_node)
    #     return all_descendants.as_frozen()

    # def find_descendant_edges(self, source_nodes: OrderedSet[NodeT]) -> MutableOrderedSet[EdgeT]:
    #     all_descendant_edges = MutableOrderedSet[EdgeT]()
    #     nodes_to_process: list[NodeT] = list(source_nodes)
    #     processed_nodes: MutableOrderedSet[NodeT] = MutableOrderedSet()
    #
    #     while True:
    #         if len(nodes_to_process) == 0:
    #             break
    #         current_node = nodes_to_process.pop(0)
    #
    #         current_node_edges = self._mf_graph.edges_with_tail_node(current_node)
    #         all_descendant_edges.update(current_node_edges)
    #
    #         for next_edge in current_node_edges:
    #             next_head_node = next_edge.head_node
    #             if next_head_node not in processed_nodes:
    #                 nodes_to_process.append(next_head_node)
    #         processed_nodes.add(current_node)
    #
    #     return all_descendant_edges
    #
    # def find_first_descendant_nodes(
    #     self,
    #     source_node: NodeT,
    #     any_match_labels: Set[MetricflowGraphLabel],
    # ) -> MutableOrderedSet[NodeT]:
    #
    #     matching_descendant_nodes = MutableOrderedSet[NodeT]()
    #     processed_nodes: MutableOrderedSet[NodeT] = MutableOrderedSet()
    #     nodes_to_process: list[NodeT] = [source_node]
    #
    #     while True:
    #         if len(nodes_to_process) == 0:
    #             break
    #         current_node = nodes_to_process.pop(0)
    #
    #         if any(label in any_match_labels for label in current_node.labels):
    #             matching_descendant_nodes.add(current_node)
    #             continue
    #
    #         for next_edge in self._mf_graph.edges_with_tail_node(current_node):
    #             next_head_node = next_edge.head_node
    #             if next_head_node not in processed_nodes:
    #                 nodes_to_process.append(next_head_node)
    #         processed_nodes.add(current_node)
    #     return matching_descendant_nodes
    #
    # @abstractmethod
    # def find_shortest_paths(
    #     self,
    #     source_nodes: OrderedSet[NodeT],
    #     target_nodes: Optional[OrderedSet[NodeT]],
    #     weight_function: EdgeWeightFunction[NodeT, EdgeT],
    #     max_path_weight: int,
    # ) -> Sequence[MetricflowGraphPath[NodeT, EdgeT]]:
    #     raise NotImplementedError()


class EdgeWeightFunction(Generic[NodeT, EdgeT]):
    def __init__(self, graph: MetricflowGraph[NodeT, EdgeT]) -> None:
        self._graph = graph

    @abstractmethod
    def weight(self, edge: EdgeT) -> Optional[int]:
        raise NotImplementedError()


class DefaultWeightFunction(EdgeWeightFunction[NodeT, EdgeT]):
    @override
    def weight(self, edge: EdgeT) -> Optional[int]:
        return edge.weight


class MetricflowGraphPath(MetricFlowPrettyFormattable, Generic[NodeT, EdgeT], ABC):
    @property
    @abstractmethod
    def edges(self) -> Sequence[EdgeT]:
        raise NotImplementedError

    @property
    @abstractmethod
    def nodes(self) -> Sequence[NodeT]:
        raise NotImplementedError

    @property
    @abstractmethod
    def weight(self) -> Optional[int]:
        raise NotImplementedError()

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        # return format_context.formatter.pretty_format([node.node_descriptor.node_name for node in self.nodes])
        return format_context.formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping={
                "nodes": [node.node_descriptor.node_name for node in self.nodes],
                "weight": self.weight,
            },
        )


@dataclass
class MutableMetricflowGraphPath(MetricflowGraphPath[NodeT, EdgeT]):
    _nodes: list[NodeT]
    _edges: list[EdgeT]
    _weight_addition_order: list[int]
    _current_weight: int
    _current_node_set: set[NodeT]
    _node_set_addition_order: list[Optional[NodeT]]

    @staticmethod
    def create(start_node: Optional[NodeT]) -> MutableMetricflowGraphPath[NodeT, EdgeT]:
        return MutableMetricflowGraphPath(
            _nodes=[start_node] if start_node is not None else [],
            _edges=[],
            _weight_addition_order=[],
            _current_weight=0,
            _current_node_set=set(),
            _node_set_addition_order=[],
        )

    @property
    def edges(self) -> Sequence[EdgeT]:
        return self._edges

    @property
    def nodes(self) -> Sequence[NodeT]:
        return self._nodes

    def _append_node(self, node: NodeT) -> None:
        self._nodes.append(node)
        if node in self._current_node_set:
            self._node_set_addition_order.append(None)
        else:
            self._current_node_set.add(node)
            self._node_set_addition_order.append(node)

    def append(self, edge: EdgeT, weight: int) -> None:
        tail_node = edge.tail_node
        head_node = edge.head_node
        if len(self._nodes) == 0:
            self._append_node(tail_node)
        self._append_node(head_node)
        self._edges.append(edge)
        self._weight_addition_order.append(weight)
        self._current_weight += weight

    @property
    def weights(self) -> Sequence[int]:
        return self._weight_addition_order

    @property
    def weight(self) -> int:
        return self._current_weight

    def pop_end(self) -> None:
        self._pop_end()

    def _pop_end(self) -> None:
        if len(self._edges) == 0:
            if len(self._nodes) == 0:
                raise RuntimeError("Can't pop an empty path")
            elif len(self._nodes) == 1:
                self._nodes.pop(-1)
            else:
                raise RuntimeError(LazyFormat("Invalid path state", nodes=self._nodes, edges=self._edges))
            return

        self._edges.pop(-1)
        self._nodes.pop(-1)
        weight = self._weight_addition_order.pop(-1)
        self._current_weight -= weight

        added_node = self._node_set_addition_order.pop(-1)
        if added_node is not None:
            self._current_node_set.remove(added_node)
        return

    @property
    def node_set(self) -> Set[NodeT]:
        return self._current_node_set


@dataclass
class NodeVisitContext(Generic[NodeT, EdgeT]):
    node: NodeT
    weight_added_by_edge_to_this_node: int
    edges_to_process_from_this_node: list[EdgeT]
