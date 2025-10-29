from __future__ import annotations

import itertools
import logging
import threading
from abc import ABC
from collections import defaultdict
from collections.abc import Set
from typing import Final, Generic, Iterator, Optional, Sequence, TypeVar

from metricflow_semantics.errors.error_classes import MetricFlowInternalError
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.toolkit.mf_graph.graph_labeling import MetricFlowGraphLabel
from metricflow_semantics.toolkit.mf_graph.mf_graph import (
    MetricFlowGraph,
)
from metricflow_semantics.toolkit.mf_graph.mutable_graph import EdgeT, NodeT
from metricflow_semantics.toolkit.mf_graph.path_finding.graph_path import MutablePathT
from metricflow_semantics.toolkit.mf_graph.path_finding.pathfinder_result import (
    FindAncestorsResult,
    FindDescendantsResult,
)
from metricflow_semantics.toolkit.mf_graph.path_finding.traversal_profile import (
    GraphTraversalProfile,
    MutableGraphTraversalProfile,
)
from metricflow_semantics.toolkit.mf_graph.path_finding.weight_function import WeightFunction
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.syntactic_sugar import mf_flatten

logger = logging.getLogger(__name__)


EdgeWeightFunctionT = TypeVar("EdgeWeightFunctionT", bound="WeightFunction")


class MetricFlowPathfinder(Generic[NodeT, EdgeT, MutablePathT], ABC):
    """Finds paths and related nodes (e.g. ancestors) via common graph algorithms.

    * This can be swapped out with another implementation.
    * `networkx` was evaluated, but there were some issues with the API (some methods did not support the required
      arguments).
    """

    _MAX_BFS_ITERATION_COUNT: Final[int] = 100

    def __init__(self) -> None:  # noqa: D107
        self._local_state = _MetricFlowPathfinderLocalState()
        self._lo = MutableGraphTraversalProfile()
        self._verbose_debug_logs = False

    def find_paths_dfs(
        self,
        graph: MetricFlowGraph[NodeT, EdgeT],
        initial_path: MutablePathT,
        target_nodes: Set[NodeT],
        weight_function: WeightFunction[NodeT, EdgeT, MutablePathT],
        max_path_weight: int,
        node_allow_set: Optional[Set[NodeT]] = None,
        node_deny_set: Optional[Set[NodeT]] = None,
        traversal_description: Optional[str] = None,
    ) -> Iterator[MutablePathT]:
        """Find paths (no cycles) to the target nodes via DFS.

        Args:
            graph: The graph to traverse.
            initial_path: The mutable path that should be extended during traversal.
            target_nodes: The nodes where the traversal should stop.
            weight_function: The weight function that should be used to compute the integer weight of a path. Edges can
            be blocked if the function returns `None`.
            max_path_weight: The maximum allowed weight of a path as computed by the provided weight function.
            node_allow_set: If specified, only these nodes will be considered for traversal. Otherwise, all nodes in the
            graph are allowed.
            node_deny_set: If specified, these nodes will be excluded from traversal.
            traversal_description: A description of the traversal for log / error messages.

        Returns: An iterator that returns all possible paths to any of the target nodes.
        """
        traversal: _DfsTraversal[NodeT, EdgeT, MutablePathT] = _DfsTraversal(
            graph=graph,
            initial_path=initial_path,
            target_nodes=target_nodes,
            weight_function=weight_function,
            max_path_weight=max_path_weight,
            node_allow_set=node_allow_set,
            node_deny_set=node_deny_set,
            traversal_profile=self._local_state.traversal_profile,
            traversal_description=traversal_description,
            verbose_debug_logs=self._verbose_debug_logs,
        )
        return traversal.find_paths()

    def find_descendants(
        self,
        graph: MetricFlowGraph[NodeT, EdgeT],
        source_nodes: OrderedSet[NodeT],
        target_nodes: OrderedSet[NodeT],
        node_allow_set: Optional[Set[NodeT]] = None,
        downward_closed: bool = False,
        deny_labels: Optional[Set[MetricFlowGraphLabel]] = None,
        max_iteration_count: int = _MAX_BFS_ITERATION_COUNT,
    ) -> FindDescendantsResult[NodeT]:
        """Find descendant nodes of the source nodes via BFS.

        Args:
            graph: The graph to traverse.
            source_nodes: Search for descendants of these nodes.
            target_nodes: When traversing the graph to find descendants, stop if any one of these nodes are reached.
            node_allow_set: If specified, only allow these nodes for traversal. Otherwise, all nodes in the graph are
            allowed.
            downward_closed: Whether to return descendants that are downward-closed with respect to the set of reachable
            nodes (source nodes + descendants).
            deny_labels: If specified, prevent traversal to edges or nodes with these labels.
            max_iteration_count: In each iteration, the descendants of a current set of nodes are added for visiting.
            This is the maximum number of iterations that should be done.

        Returns: A result object containing the descendants and other context.

        """
        # In each iteration, evaluate these nodes.
        batch_of_nodes_to_evaluate = list(source_nodes)
        # The target nodes that were reachable from at least one of the source nodes.
        reached_target_nodes: MutableOrderedSet[NodeT] = MutableOrderedSet()
        # All reachable nodes found during traversal.
        reachable_nodes: MutableOrderedSet[NodeT] = MutableOrderedSet(source_nodes)

        if self._verbose_debug_logs:
            logger.debug(LazyFormat("Finding descendants.", source_nodes=source_nodes, target_nodes=target_nodes))
        # Maps a descendant node to the source nodes that it is reachable from.
        node_to_reachable_source_nodes: dict[NodeT, MutableOrderedSet[NodeT]] = defaultdict(MutableOrderedSet)
        for node in source_nodes:
            node_to_reachable_source_nodes[node].add(node)

        iteration_index = 0
        labels_collected_during_traversal: set[MetricFlowGraphLabel] = set()
        labels_collected_during_traversal.update(*(source_node.labels for source_node in source_nodes))

        while iteration_index <= max_iteration_count:
            if len(batch_of_nodes_to_evaluate) == 0:
                break

            next_batch_of_node_to_evaluate: list[NodeT] = []

            examined_edge_count = 0
            for current_node in batch_of_nodes_to_evaluate:
                if current_node in target_nodes:
                    reached_target_nodes.add(current_node)
                    continue

                edges_to_examine = graph.edges_with_tail_node(current_node)

                for edge in edges_to_examine:
                    traversal_labels = edge.labels_for_path_addition
                    if deny_labels and (deny_labels & traversal_labels):
                        continue

                    head_node = edge.head_node
                    if node_allow_set is not None and head_node not in node_allow_set:
                        continue

                    if downward_closed and not all(
                        predecessor_of_head_node in reachable_nodes
                        for predecessor_of_head_node in graph.predecessors(head_node)
                    ):
                        continue

                    node_to_reachable_source_nodes[head_node].update(node_to_reachable_source_nodes[current_node])
                    reachable_nodes.add(head_node)
                    next_batch_of_node_to_evaluate.append(head_node)
                    labels_collected_during_traversal.update(traversal_labels)

                examined_edge_count += len(edges_to_examine)

            self._local_state.traversal_profile.increment_edge_examined_count(examined_edge_count)
            self._local_state.traversal_profile.increment_node_visit_count(len(batch_of_nodes_to_evaluate))
            batch_of_nodes_to_evaluate = next_batch_of_node_to_evaluate
            iteration_index += 1

        return FindDescendantsResult(
            reachable_nodes=reachable_nodes,
            reachable_target_nodes=reached_target_nodes,
            labels_collected_during_traversal=FrozenOrderedSet(sorted(labels_collected_during_traversal)),
            finish_iteration_index=iteration_index,
            target_node_to_reachable_source_nodes={
                target_node: node_to_reachable_source_nodes[target_node] for target_node in reached_target_nodes
            },
        )

    def find_ancestors(
        self,
        graph: MetricFlowGraph[NodeT, EdgeT],
        source_nodes: OrderedSet[NodeT],
        target_nodes: OrderedSet[NodeT],
        node_allow_set: Optional[Set[NodeT]] = None,
        deny_labels: Optional[Set[MetricFlowGraphLabel]] = None,
        upwards_closed: bool = False,
        max_iteration_count: int = _MAX_BFS_ITERATION_COUNT,
    ) -> FindAncestorsResult[NodeT]:
        """Find the ancestors of the source nodes.

        The arguments to this method are similar to the ones for `find_descendants`.
        """
        batch_of_nodes_to_evaluate: list[NodeT] = list(target_nodes)

        if self._verbose_debug_logs:
            logger.debug(LazyFormat("Finding ancestors.", source_nodes=source_nodes, target_nodes=target_nodes))

        iteration_index = 0

        labels_collected_during_traversal: set[MetricFlowGraphLabel] = set(
            mf_flatten(target_node.labels for target_node in target_nodes)
        )
        reachable_nodes: MutableOrderedSet[NodeT] = MutableOrderedSet(source_nodes)
        reachable_nodes.update(target_nodes)

        node_to_reachable_target_nodes: dict[NodeT, MutableOrderedSet[NodeT]] = defaultdict(MutableOrderedSet)

        for target_node in target_nodes:
            node_to_reachable_target_nodes[target_node].add(target_node)

        reachable_source_nodes: MutableOrderedSet[NodeT] = MutableOrderedSet()

        while iteration_index <= max_iteration_count:
            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Starting iteration",
                        reachable_nodes=batch_of_nodes_to_evaluate,
                    )
                )
            if len(batch_of_nodes_to_evaluate) == 0:
                break

            next_batch_of_node_to_evaluate: list[NodeT] = []
            examined_edges_count = 0
            for current_node in batch_of_nodes_to_evaluate:
                if current_node in source_nodes:
                    reachable_source_nodes.add(current_node)
                    continue

                predecessor_nodes_of_batch: Set[NodeT] = set(
                    mf_flatten(graph.predecessors(node) for node in batch_of_nodes_to_evaluate)
                )
                excluded_predecessor_nodes: set[NodeT] = set()
                for predecessor_node in predecessor_nodes_of_batch:
                    if node_allow_set is not None and predecessor_node not in node_allow_set:
                        if self._verbose_debug_logs:
                            logger.debug(
                                LazyFormat(
                                    "Excluding node as it's not in the allowed list", predecessor_node=predecessor_node
                                )
                            )
                        excluded_predecessor_nodes.add(predecessor_node)
                        continue

                    if deny_labels is not None and (deny_labels & predecessor_node.labels):
                        excluded_predecessor_nodes.add(predecessor_node)
                        if self._verbose_debug_logs:
                            logger.debug(
                                LazyFormat(
                                    "Excluding node as it matches the deny label set.",
                                    predecessor_node=predecessor_node,
                                    deny_labels=deny_labels,
                                )
                            )
                        continue

                    if upwards_closed and any(
                        successor_of_predecessor not in reachable_nodes
                        for successor_of_predecessor in graph.successors(predecessor_node)
                    ):
                        excluded_predecessor_nodes.add(predecessor_node)
                        if self._verbose_debug_logs:
                            logger.debug(
                                LazyFormat(
                                    "Excluding node as it's not upwards-closed.",
                                    predecessor_node=predecessor_node,
                                    successors=graph.successors(predecessor_node),
                                )
                            )
                        continue
                if self._verbose_debug_logs and excluded_predecessor_nodes:
                    logger.debug(
                        LazyFormat(
                            "Excluding selected predecessor nodes of this batch",
                            excluded_predecessor_nodes=excluded_predecessor_nodes,
                        )
                    )
                edges_to_examine = graph.edges_with_head_node(current_node)
                for edge_from_predecessor in graph.edges_with_head_node(current_node):
                    predecessor_node = edge_from_predecessor.tail_node
                    if predecessor_node in excluded_predecessor_nodes:
                        continue

                    next_batch_of_node_to_evaluate.append(predecessor_node)
                    labels_collected_during_traversal.update(
                        itertools.chain(edge_from_predecessor.labels, predecessor_node.labels)
                    )
                    reachable_nodes.add(predecessor_node)

                    node_to_reachable_target_nodes[predecessor_node].update(
                        node_to_reachable_target_nodes[edge_from_predecessor.head_node]
                    )
                examined_edges_count += len(edges_to_examine)

            self._local_state.traversal_profile.increment_edge_examined_count(examined_edges_count)
            self._local_state.traversal_profile.increment_node_visit_count(len(batch_of_nodes_to_evaluate))
            batch_of_nodes_to_evaluate = next_batch_of_node_to_evaluate
            iteration_index += 1

        return FindAncestorsResult(
            reachable_nodes=reachable_nodes,
            reachable_source_nodes=reachable_source_nodes,
            source_node_to_reachable_target_nodes={
                source_node: node_to_reachable_target_nodes[source_node] for source_node in reachable_source_nodes
            },
            labels_collected_during_traversal=FrozenOrderedSet(labels_collected_during_traversal),
            finish_iteration_index=iteration_index,
        )

    @property
    def traversal_profile_snapshot(self) -> GraphTraversalProfile:
        """Return a snapshot of the current counter set for pathfinder metrics.

        The counter set is used for logging / debugging.
        """
        return self._local_state.traversal_profile.copy()


class _DfsTraversal(Generic[NodeT, EdgeT, MutablePathT]):
    """A context object that contains shared state for a specific DFS traversal."""

    def __init__(
        self,
        graph: MetricFlowGraph[NodeT, EdgeT],
        initial_path: MutablePathT,
        target_nodes: Set[NodeT],
        weight_function: WeightFunction[NodeT, EdgeT, MutablePathT],
        max_path_weight: int,
        node_allow_set: Optional[Set[NodeT]],
        node_deny_set: Optional[Set[NodeT]],
        traversal_profile: MutableGraphTraversalProfile,
        traversal_description: Optional[str],
        verbose_debug_logs: bool,
    ) -> None:
        """See `find_paths_dfs` for description of the arguments."""
        if initial_path.is_empty:
            raise MetricFlowInternalError(
                LazyFormat(
                    "The initial path must have at least one node. Otherwise, there's no way to extend it.",
                    traversal_description=traversal_description,
                )
            )
        self._traversal_description = traversal_description
        self._current_path = initial_path
        self._graph = graph
        self._target_nodes = target_nodes
        self._weight_function = weight_function
        self._max_path_weight = max_path_weight
        self._node_allow_set = node_allow_set
        self._node_deny_set = node_deny_set
        self._traversal_profile = traversal_profile
        self._verbose_debug_logs = verbose_debug_logs

    def _get_valid_next_edges(self, current_node: NodeT) -> Sequence[tuple[EdgeT, int]]:
        """For a given node, figure out the next valid edges for the node."""
        current_weight = self._current_path.weight
        node_allow_set = self._node_allow_set
        node_deny_set = self._node_deny_set

        valid_next_edges: list[tuple[EdgeT, int]] = []
        candidate_edges = self._graph.edges_with_tail_node(current_node)
        for candidate_edge in candidate_edges:
            next_node = candidate_edge.head_node

            if node_allow_set is not None and next_node not in node_allow_set:
                continue

            if node_deny_set is not None and next_node in node_deny_set:
                continue

            # Block cycles.
            if candidate_edge.head_node in self._current_path.node_set:
                continue

            weight_added_by_candidate_edge = self._weight_function.incremental_weight(
                path_to_node=self._current_path,
                next_edge=candidate_edge,
            )

            if weight_added_by_candidate_edge is None:
                continue

            if current_weight + weight_added_by_candidate_edge > self._max_path_weight:
                continue

            valid_next_edges.append((candidate_edge, weight_added_by_candidate_edge))
        self._traversal_profile.increment_edge_examined_count(len(candidate_edges))
        return valid_next_edges

    def find_paths(self) -> Iterator[MutablePathT]:
        traversal_start_node = self._current_path.nodes[-1]
        return self._traverse_dfs(traversal_start_node)

    def _traverse_dfs(self, current_node: NodeT) -> Iterator[MutablePathT]:
        if self._verbose_debug_logs:
            logger.debug(LazyFormat("Visiting node", current_node=current_node, current_path=self._current_path))
        self._traversal_profile.increment_node_visit_count()
        current_path = self._current_path

        if current_node in self._target_nodes:
            self._traversal_profile.increment_generated_paths_count()
            yield current_path
            return

        for next_edge, incremental_weight in self._get_valid_next_edges(current_node):
            current_path.append_edge(next_edge, incremental_weight)
            for path in self._traverse_dfs(next_edge.head_node):
                yield path
            current_path.pop_end()


class _MetricFlowPathfinderLocalState(threading.local):
    def __init__(self) -> None:  # noqa: D107
        self.traversal_profile = MutableGraphTraversalProfile()
