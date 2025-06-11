from __future__ import annotations

import logging
from abc import ABC
from collections.abc import Generator, Set
from dataclasses import dataclass
from typing import Generic, Optional, TypeVar

from metricflow_semantics.experimental.mf_graph.mf_graph import (
    MetricflowGraph,
)
from metricflow_semantics.experimental.mf_graph.mutable_graph import EdgeT, NodeT
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet
from metricflow_semantics.experimental.semantic_graph.path_finding.graph_path import PathT
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder_cache import (
    FindCommonReachableTargetsCacheKey,
    PathFinderCache,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder_result import (
    FindDescendantsResult,
    FindReachableTargetsResult,
    FindReachableTargetsSimpleResult,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder_stat import MutablePathFinderStat
from metricflow_semantics.experimental.semantic_graph.path_finding.traversal_event import (
    StopPathExplorationEvent,
    StopPathExplorationReason,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.weight_function import WeightFunction
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


EdgeWeightFunctionT = TypeVar("EdgeWeightFunctionT", bound="WeightFunction")


class MetricflowGraphPathFinder(Generic[NodeT, EdgeT, PathT], ABC):
    def __init__(
        self,
        path_finder_cache: PathFinderCache[NodeT, EdgeT, PathT],
    ) -> None:
        self._path_finder_cache = path_finder_cache

        # Variables for `_traverse_dfs`.
        self._finished_visiting_nodes: MutableOrderedSet[NodeT] = MutableOrderedSet()
        self._node_visit_contexts: list[TraversalVisitContext] = []
        self._mutable_path: Optional[PathT] = None
        self._cumulative_stat = MutablePathFinderStat()

        self._verbose_debug_logs = True

    def _current_mutable_path(self) -> PathT:
        if self._mutable_path is None:
            raise RuntimeError("`_mutable_path` should have been set before calling this method.")
        return self._mutable_path

    def _current_mutable_path_length(self) -> int:
        return len(self._current_mutable_path())

    def _traverse_dfs__walk_to_previous_node(self) -> None:
        self._node_visit_contexts.pop()
        self._finished_visiting_nodes.add(self._current_mutable_path().nodes[-1])
        self._current_mutable_path().pop()

    def _traverse_dfs__walk_via_edge(
        self,
        edge_to_take: EdgeT,
        weight_added_by_edge: int,
        edges_to_visit: list[EdgeT],
    ) -> None:
        self._node_visit_contexts.append(
            TraversalVisitContext(
                node=edge_to_take.head_node,
                weight_added_by_edge_to_this_node=weight_added_by_edge,
                edges_to_process_from_this_node=edges_to_visit,
            )
        )
        self._current_mutable_path().append_edge(edge_to_take, weight_added_by_edge)
        self._cumulative_stat.increment_node_visit_count()

    def find_descendant_nodes(
        self,
        graph: MetricflowGraph[NodeT, EdgeT],
        mutable_path: PathT,
        source_node: NodeT,
        candidate_target_nodes: Set[NodeT],
        weight_function: WeightFunction[NodeT, EdgeT, PathT],
        max_path_weight: int,
    ) -> FindDescendantsResult[NodeT]:
        start_stat = self._cumulative_stat.copy()
        descendant_nodes = MutableOrderedSet[NodeT]()

        for stop_exploration_event in self.traverse_dfs(
            graph=graph,
            mutable_path=mutable_path,
            source_node=source_node,
            target_nodes=candidate_target_nodes,
            weight_function=weight_function,
            max_path_weight=max_path_weight,
            allow_node_revisits=False,
        ):
            current_path = stop_exploration_event.current_path
            descendant_nodes.update(current_path.node_set)

        return FindDescendantsResult(
            path_finder_stat=self._cumulative_stat.difference(start_stat),
            descendant_nodes=descendant_nodes,
        )

    def find_reachable_targets_simple(
        self,
        graph: MetricflowGraph[NodeT, EdgeT],
        mutable_path: PathT,
        source_node: NodeT,
        candidate_target_nodes: Set[NodeT],
        weight_function: WeightFunction[NodeT, EdgeT, PathT],
        max_path_weight: int,
    ) -> FindReachableTargetsSimpleResult:
        start_stat = self._cumulative_stat.copy()
        found_target_nodes = MutableOrderedSet[NodeT]()

        for stop_event in self.traverse_dfs(
            graph=graph,
            mutable_path=mutable_path,
            source_node=source_node,
            target_nodes=candidate_target_nodes,
            weight_function=weight_function,
            max_path_weight=max_path_weight,
            allow_node_revisits=False,
        ):
            last_node_in_path = stop_event.current_path.nodes[-1]
            if last_node_in_path in candidate_target_nodes:
                found_target_nodes.add(last_node_in_path)

        return FindReachableTargetsSimpleResult(
            path_finder_stat=self._cumulative_stat.difference(start_stat),
            reachable_targets=found_target_nodes.as_frozen(),
        )

    def traverse_dfs(
        self,
        graph: MetricflowGraph[NodeT, EdgeT],
        mutable_path: PathT,
        source_node: NodeT,
        target_nodes: Set[NodeT],
        weight_function: WeightFunction[NodeT, EdgeT, PathT],
        max_path_weight: int,
        allow_node_revisits: bool,
        # allow_simple_cycle: bool,
    ) -> Generator[StopPathExplorationEvent, None, None]:
        # Visit the descendants in DFS, starting from the source node.
        mutable_path.reset_to_start_node(source_node)
        self._mutable_path = mutable_path

        self._finished_visiting_nodes = MutableOrderedSet()
        self._node_visit_contexts = [
            TraversalVisitContext(
                node=source_node,
                weight_added_by_edge_to_this_node=0,
                edges_to_process_from_this_node=list(graph.edges_with_tail_node(source_node)),
            )
        ]

        found_target_nodes: MutableOrderedSet[NodeT] = MutableOrderedSet()

        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Starting DFS traversal",
                    graph=graph,
                    source_node=source_node,
                    target_nodes=target_nodes,
                    weight_function=weight_function,
                    max_path_weight=max_path_weight,
                    allow_node_revisits=allow_node_revisits,
                )
            )

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
            current_path = self._current_mutable_path()

            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Evaluating next node in traversal",
                        current_node=current_node.node_descriptor.node_name,
                        current_node_visit_context=current_node_visit_context,
                        current_path=current_path,
                    )
                )

            # If we've hit one of the target nodes, so return the path to the node and stop visiting
            # descendants of the current node.
            # The path length check is to allow for simple cycles.
            # TODO: Allowing simple cycles should be an option.
            if current_node in target_nodes and len(current_path.nodes) != 1:
                if self._verbose_debug_logs:
                    logger.debug(
                        LazyFormat(
                            "Reached target node, so returning current path",
                            current_node=current_node,
                            current_path=current_path,
                        )
                    )
                self._cumulative_stat.increment_generated_paths_count()
                yield StopPathExplorationEvent(
                    stop_reason=StopPathExplorationReason.VISIT_TARGET_NODE,
                    current_path=current_path,
                )
                found_target_nodes.add(current_node)

                # Stop traversal if we've found all target nodes.
                if not allow_node_revisits and len(found_target_nodes) == len(target_nodes):
                    if self._verbose_debug_logs:
                        logger.debug(
                            LazyFormat(
                                "Stopping traversal as all target nodes have been found",
                                allow_node_revisits=allow_node_revisits,
                            )
                        )
                    return
                self._traverse_dfs__walk_to_previous_node()
                continue

            # If we can't go to the next node, then restart the loop so that we can check the next edge.
            if not allow_node_revisits and current_node in self._finished_visiting_nodes:
                if self._verbose_debug_logs:
                    logger.debug(
                        LazyFormat(
                            "Not exploring descendant edges as we've previously finished" " visiting the current node",
                            current_node=current_node,
                            finished_visiting_nodes=self._finished_visiting_nodes,
                        )
                    )
                yield StopPathExplorationEvent(
                    stop_reason=StopPathExplorationReason.VISIT_FINISHED_NODE,
                    current_path=current_path,
                )
                self._traverse_dfs__walk_to_previous_node()
                continue

            # Handle cycles
            if current_node in current_path.node_set and current_path.nodes[-1] != current_node:
                if self._verbose_debug_logs:
                    logger.debug(
                        LazyFormat(
                            "Skipping node as it would produce a cycle.",
                            skipped_node=current_node,
                            current_path=self._current_mutable_path(),
                        )
                    )
                self._traverse_dfs__walk_to_previous_node()
                continue

            # If the current node has no descendants, then go to the next visit context.
            edges_to_process_in_current_node = current_node_visit_context.edges_to_process_from_this_node
            if len(edges_to_process_in_current_node) == 0:
                if self._verbose_debug_logs:
                    logger.debug(LazyFormat("No more edges remaining for current context, so popping it off."))
                self._traverse_dfs__walk_to_previous_node()
                continue

            # See if we can go from the current node to the next descendant node.
            next_edge_to_take = edges_to_process_in_current_node.pop()
            self._cumulative_stat.increment_edge_examined_count()

            next_edge_weight = weight_function.incremental_weight(self._current_mutable_path(), next_edge_to_take)

            if next_edge_weight is None:
                if self._verbose_debug_logs:
                    logger.debug(
                        LazyFormat(
                            "Skipping edge as the weight is not set",
                            edge=next_edge_to_take,
                            current_path=self._current_mutable_path(),
                        )
                    )
                continue

            current_path_weight = self._current_mutable_path().weight

            if next_edge_weight + current_path_weight > max_path_weight:
                if self._verbose_debug_logs:
                    logger.debug(
                        LazyFormat(
                            "Skipping edge as the weight would exceed cutoff",
                            edge=next_edge_to_take,
                            edge_weight=next_edge_weight,
                            current_path_weight=current_path_weight,
                        )
                    )
                continue

            # Take the next edge.
            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Taking edge", next_edge_to_take=next_edge_to_take, current_path=self._current_mutable_path()
                    )
                )
            self._traverse_dfs__walk_via_edge(
                edge_to_take=next_edge_to_take,
                weight_added_by_edge=next_edge_weight,
                edges_to_visit=list(
                    graph.edges_with_tail_node(next_edge_to_take.head_node),
                ),
            )

        return

    def find_common_reachable_targets(
        self,
        graph: MetricflowGraph[NodeT, EdgeT],
        mutable_path: PathT,
        source_nodes: FrozenOrderedSet[NodeT],
        candidate_target_nodes: FrozenOrderedSet[NodeT],
        weight_function: WeightFunction[NodeT, EdgeT, PathT],
        max_path_weight: int,
    ) -> FindReachableTargetsResult[NodeT]:
        cache = self._path_finder_cache.find_common_reachable_targets_cache
        cache_key = FindCommonReachableTargetsCacheKey(
            graph_id=graph.graph_id,
            source_nodes=source_nodes,
            candidate_target_nodes=candidate_target_nodes,
            weight_function=weight_function,
            max_path_weight=max_path_weight,
        )
        start_stat = self._cumulative_stat.copy()
        result_using_cache = cache.get_or_create(
            cache_key=cache_key,
            factory=lambda: self._find_common_reachable_targets_non_cached(
                graph=graph,
                mutable_path=mutable_path,
                source_nodes=source_nodes,
                candidate_target_nodes=candidate_target_nodes,
                weight_function=weight_function,
                max_path_weight=max_path_weight,
            ),
        )
        return FindReachableTargetsResult(
            path_finder_stat=self._cumulative_stat.difference(start_stat),
            descendant_nodes=result_using_cache.descendant_nodes,
            reachable_targets=result_using_cache.reachable_targets,
        )

    def _find_common_reachable_targets_non_cached(
        self,
        graph: MetricflowGraph[NodeT, EdgeT],
        mutable_path: PathT,
        source_nodes: FrozenOrderedSet[NodeT],
        candidate_target_nodes: FrozenOrderedSet[NodeT],
        weight_function: WeightFunction[NodeT, EdgeT, PathT],
        max_path_weight: int,
    ) -> FindReachableTargetsResult[NodeT]:
        common_reachable_targets = MutableOrderedSet[NodeT]()
        descendant_nodes = MutableOrderedSet[NodeT]()
        start_stat = self._cumulative_stat.copy()
        for i, source_node in enumerate(source_nodes):
            find_descendant_nodes_result = self.find_descendant_nodes(
                graph=graph,
                mutable_path=mutable_path,
                source_node=source_node,
                candidate_target_nodes=candidate_target_nodes,
                weight_function=weight_function,
                max_path_weight=max_path_weight,
            )
            reachable_targets = find_descendant_nodes_result.descendant_nodes.intersection(candidate_target_nodes)
            descendant_nodes.update(find_descendant_nodes_result.descendant_nodes)
            if i == 0:
                common_reachable_targets = reachable_targets.copy()

            common_reachable_targets = common_reachable_targets.intersection(reachable_targets)

            if len(common_reachable_targets) == 0:
                descendant_nodes.clear()
                break

            # TODO: Could improve performance by using a lookup for nodes along the same path.

        return FindReachableTargetsResult(
            path_finder_stat=self._cumulative_stat.difference(start_stat),
            descendant_nodes=descendant_nodes,
            reachable_targets=common_reachable_targets,
        )


@dataclass
class TraversalVisitContext(Generic[NodeT, EdgeT]):
    node: NodeT
    weight_added_by_edge_to_this_node: int
    edges_to_process_from_this_node: list[EdgeT]
