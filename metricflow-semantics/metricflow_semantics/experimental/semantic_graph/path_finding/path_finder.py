from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Set
from dataclasses import dataclass
from typing import Generator, Generic, Optional, TypeVar, override

from metricflow_semantics.experimental.mf_graph.mf_graph import (
    MetricflowGraph,
)
from metricflow_semantics.experimental.mf_graph.mutable_graph import EdgeT, NodeT
from metricflow_semantics.experimental.ordered_set import MutableOrderedSet
from metricflow_semantics.experimental.semantic_graph.path_finding.graph_path import (
    MutableMetricflowGraphPath,
)
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


EdgeWeightFunctionT = TypeVar("EdgeWeightFunctionT", bound="WeightFunction")


@dataclass
class FindDescendantResult(Generic[NodeT, EdgeT]):
    descendant_nodes: MutableOrderedSet[NodeT]
    found_target_nodes: MutableOrderedSet[NodeT]


PathT = TypeVar("PathT", bound=MutableMetricflowGraphPath)


class MetricflowGraphPathFinder(Generic[NodeT, EdgeT], ABC):
    def __init__(self, graph: MetricflowGraph[NodeT, EdgeT]) -> None:
        self._mf_graph = graph

        # Variables for `_traverse_dfs`.
        self._finished_visiting_nodes: MutableOrderedSet[NodeT] = MutableOrderedSet()
        self._node_visit_contexts: list[NodeVisitContext] = []
        self._logging_enabled = False

    def _pop_node_visit_context(self, mutable_path: MutableMetricflowGraphPath[NodeT, EdgeT]) -> None:
        self._node_visit_contexts.pop(-1)
        # self._finished_visiting_nodes.add(current_path.nodes[-1])
        # self._node_visit_contexts.pop()

        self._finished_visiting_nodes.add(mutable_path.nodes[-1])
        mutable_path.pop()

    def _push_node_visit_context(
        self,
        edge_to_take: EdgeT,
        weight_added_by_edge: int,
        edges_to_visit: list[EdgeT],
        mutable_path: MutableMetricflowGraphPath[NodeT, EdgeT],
    ) -> None:
        self._node_visit_contexts.append(
            NodeVisitContext(
                node=edge_to_take.head_node,
                weight_added_by_edge_to_this_node=weight_added_by_edge,
                edges_to_process_from_this_node=edges_to_visit,
            )
        )
        mutable_path.append_edge(edge_to_take, weight_added_by_edge)

    # def find_all_simple_paths(
    #     self,
    #     source_node: NodeT,
    #     target_nodes: Set[NodeT],
    #     weight_function: WeightFunction[NodeT, EdgeT, PathT],
    #     max_path_weight: int,
    #     mutable_path: Optional[PathT] = None,
    # ) -> Generator[MetricflowGraphPath[NodeT, EdgeT], None, None]:
    #     mutable_path = mutable_path or MutableMetricflowGraphPath.create()
    #     for path in self.traverse_dfs(
    #         source_node=source_node,
    #         target_nodes=target_nodes,
    #         weight_function=weight_function,
    #         max_path_weight=max_path_weight,
    #         allow_node_revisits=True,
    #         mutable_path=mutable_path,
    #     ):
    #         yield path

    def find_descendant_nodes(
        self,
        source_node: NodeT,
        candidate_target_nodes: Set[NodeT],
        weight_function: WeightFunction[NodeT, EdgeT, PathT],
        max_path_weight: int,
        mutable_path: PathT,
    ) -> FindDescendantResult[NodeT, EdgeT]:
        descendant_nodes = MutableOrderedSet[NodeT]()
        found_target_nodes = MutableOrderedSet[NodeT]()

        for path in self.traverse_dfs(
            source_node=source_node,
            target_nodes=candidate_target_nodes,
            weight_function=weight_function,
            max_path_weight=max_path_weight,
            allow_node_revisits=False,
            mutable_path=mutable_path,
        ):
            descendant_nodes.update(path.node_set)
            found_target_nodes.add(path.nodes[-1])

        return FindDescendantResult(
            descendant_nodes=descendant_nodes,
            found_target_nodes=found_target_nodes,
        )

    def traverse_dfs(
        self,
        source_node: NodeT,
        target_nodes: Set[NodeT],
        weight_function: WeightFunction[NodeT, EdgeT, PathT],
        max_path_weight: int,
        allow_node_revisits: bool,
        mutable_path: PathT,
    ) -> Generator[PathT, None, None]:
        # Visit the descendants in DFS, starting from the source node.
        self._finished_visiting_nodes = MutableOrderedSet()
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

            self._log(
                LazyFormat(
                    "Start visit", current_node=current_node, current_node_visit_context=current_node_visit_context
                )
            )

            # If we've hit one of the target nodes, so return the path to the node and stop visiting
            # descendants of the current node.
            if current_node in target_nodes:
                self._log(LazyFormat("Reached target node, so returning current path", current_node=current_node))
                yield mutable_path
                self._pop_node_visit_context(mutable_path)
                # mutable_path.pop()
                continue

            # If the current node has no descendants, then go to the next visit context.
            edges_to_process_in_current_node = current_node_visit_context.edges_to_process_from_this_node
            if len(edges_to_process_in_current_node) == 0:
                self._log(LazyFormat("No more edges remaining for current context, so popping it off."))

                self._pop_node_visit_context(mutable_path)
                # mutable_path.pop()
                continue

            # See if we can go from the current node to the next descendant node.
            next_edge_to_take = edges_to_process_in_current_node.pop(-1)
            next_node = next_edge_to_take.head_node

            # If we can't go to the next node, then restart the loop so that we can check the next edge.
            if not allow_node_revisits and next_node in self._finished_visiting_nodes:
                self._log(LazyFormat("Skipping node as it has already been visited.", next_node=next_node))
                continue

            # Avoid cycles
            if next_node in mutable_path.node_set:
                self._log(LazyFormat("Skipping node as would produce a cycle.", next_node=next_node))
                continue

            next_edge_weight = weight_function.weight(mutable_path, next_edge_to_take)

            if next_edge_weight is None:
                self._log(LazyFormat("Skipping edge as the weight is not set", next_edge_to_take=next_edge_to_take))
                continue

            if next_edge_weight + mutable_path.weight > max_path_weight:
                self._log(
                    LazyFormat(
                        "Skipping edge as the weight would exceed cutoff",
                        next_edge_to_take=next_edge_to_take,
                        next_edge_weight=next_edge_weight,
                    )
                )
                continue

            # Take the next edge.
            # mutable_path.append_edge(next_edge_to_take, next_edge_weight)
            self._push_node_visit_context(
                edge_to_take=next_edge_to_take,
                weight_added_by_edge=next_edge_weight,
                edges_to_visit=list(
                    self._mf_graph.edges_with_tail_node(next_edge_to_take.head_node),
                ),
                mutable_path=mutable_path,
            )

        return

    def _log(self, message: LazyFormat) -> None:
        if self._logging_enabled:
            logger.debug(message)


class WeightFunction(Generic[NodeT, EdgeT, PathT], ABC):
    @abstractmethod
    def weight(self, path: PathT, edge: EdgeT) -> Optional[int]:
        raise NotImplementedError()


@dataclass
class NodeVisitContext(Generic[NodeT, EdgeT]):
    node: NodeT
    weight_added_by_edge_to_this_node: int
    edges_to_process_from_this_node: list[EdgeT]


class DefaultWeightFunction(WeightFunction[NodeT, EdgeT, MutableMetricflowGraphPath]):
    @override
    def weight(self, path: MutableMetricflowGraphPath[NodeT, EdgeT], edge: EdgeT) -> Optional[int]:
        return 1
