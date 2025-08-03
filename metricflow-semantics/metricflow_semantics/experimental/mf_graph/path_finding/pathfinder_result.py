from __future__ import annotations

import logging
from abc import ABC
from dataclasses import dataclass
from typing import Generic, Mapping

from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.mf_graph.mutable_graph import NodeT
from metricflow_semantics.experimental.mf_graph.path_finding.traversal_profile import GraphTraversalProfile
from metricflow_semantics.experimental.ordered_set import OrderedSet

logger = logging.getLogger(__name__)


@dataclass
class GraphTraversalResult(ABC):
    """A mixin dataclass that can be used to include the pathfinder counters in result objects."""

    traversal_profile: GraphTraversalProfile


@dataclass
class FindDescendantsResult(Generic[NodeT]):
    """A result object used to return the results of the `find_descendants` method."""

    # All nodes on any path from any source node to any target node.
    reachable_nodes: OrderedSet[NodeT]
    # The (sub)set of target nodes that were reachable from the source nodes.
    reachable_target_nodes: OrderedSet[NodeT]
    # The labels on edges / nodes that are on the path from any of the source nodes to any of the target nodes.
    labels_collected_during_traversal: OrderedSet[MetricflowGraphLabel]
    # The number of iterations that were done to reach the target nodes or hit the iteration limit.
    finish_iteration_index: int
    # Maps the target node to the set of source nodes that could be reached.
    target_node_to_reachable_source_nodes: dict[NodeT, OrderedSet[NodeT]]


@dataclass
class FindAncestorsResult(Generic[NodeT]):
    """A result object used to return the results of the `find_ancestors` method."""

    # All nodes on any path from any source node to any target node.
    reachable_nodes: OrderedSet[NodeT]
    # The (sub)set of source nodes that were reachable from the source nodes.
    reachable_source_nodes: OrderedSet[NodeT]
    # Maps the source node to the target nodes that could be reached.
    source_node_to_reachable_target_nodes: Mapping[NodeT, OrderedSet[NodeT]]
    # The labels on edges / nodes that are on the path from any of the source nodes to any of the target nodes.
    labels_collected_during_traversal: OrderedSet[MetricflowGraphLabel]
    # The number of iterations that were done to reach the target nodes or hit the iteration limit.
    finish_iteration_index: int
