from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Generic, Optional, Sequence, Sized, TypeVar, override

from typing_extensions import override

from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraphEdge, MetricflowGraphNode
from metricflow_semantics.experimental.ordered_set import MutableOrderedSet, OrderedSet
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.mf_logging.pretty_formatter import PrettyFormatContext

logger = logging.getLogger(__name__)


NodeT = TypeVar("NodeT", bound=MetricflowGraphNode)
EdgeT = TypeVar("EdgeT", bound=MetricflowGraphEdge)


class MetricflowGraphPath(MetricFlowPrettyFormattable, Sized, Generic[NodeT, EdgeT], ABC):
    """A read-only interface that describes a path in a directed graph."""

    @property
    @abstractmethod
    def edges(self) -> Sequence[EdgeT]:
        """The edges (in order) that constitute this path."""
        raise NotImplementedError

    @property
    @abstractmethod
    def nodes(self) -> Sequence[NodeT]:
        """The nodes (in order) that constitute this path."""
        raise NotImplementedError

    @property
    @abstractmethod
    def weight(self) -> Optional[int]:
        """The weight of this path as defined by the weight function that was used during traversal."""
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

    @property
    @abstractmethod
    def node_set(self) -> OrderedSet[NodeT]:
        """The set of nodes in this path. Useful for fast checks for cycles during path extension."""
        raise NotImplementedError()

    @override
    def __len__(self) -> int:
        return len(self.nodes)


@dataclass
class MutableGraphPath(MetricflowGraphPath[NodeT, EdgeT]):
    """A path that can be extended and also reverted back to the previous state before extension.

    * This is a mutable class as path-finding can traverse many edges, and using a single mutable object reduces
      overhead.
    * The append / pop functionality is useful for DFS traversal.
    * When an edge is added to the path, the incremental weight added by the edge is specified by the caller (this
      does not do any weight calculation).
    """

    _nodes: list[NodeT]
    _edges: list[EdgeT]
    _current_weight: int
    _current_node_set: MutableOrderedSet[NodeT]

    # As this path is extended step-by-step, keep track of the weights added so that when `pop()` is called, the weight
    # of the path afterward can be easily computed by subtracting the last incremental weight added. Similar situation
    # for `_node_set_addition_order`.
    _weight_addition_order: list[int]
    _node_set_addition_order: list[Optional[NodeT]]

    @staticmethod
    def create() -> MutableGraphPath:  # noqa: D102
        return MutableGraphPath(
            _nodes=[],
            _edges=[],
            _weight_addition_order=[],
            _current_weight=0,
            _current_node_set=MutableOrderedSet(),
            _node_set_addition_order=[],
        )

    def reset_to_start_node(self, start_node: NodeT) -> None:
        """Update the state of the path so that it is of length 1 with just the given node."""
        self._nodes = []
        self._edges = []
        self._weight_addition_order = []
        self._current_weight = 0
        self._current_node_set = MutableOrderedSet()
        self._node_set_addition_order = []

        self._append_node(start_node)

    @property
    def edges(self) -> Sequence[EdgeT]:  # noqa: D102
        return self._edges

    @property
    def nodes(self) -> Sequence[NodeT]:  # noqa: D102
        return self._nodes

    def _append_node(self, node: NodeT) -> None:
        """Helper to add a node to the path."""
        self._nodes.append(node)
        if node in self._current_node_set:
            self._node_set_addition_order.append(None)
        else:
            self._current_node_set.add(node)
            self._node_set_addition_order.append(node)
        self._node_addition_callback(node)

    def _node_addition_callback(self, node: NodeT) -> None:
        """A callback method that is called whenever a node is added to this path.

        This is useful for subclasses that want to update other state / property associated with the path.
        """
        pass

    def _edge_addition_callback(self, edge: EdgeT) -> None:
        """Similar to `_node_addition_callback` but for edge additions."""
        pass

    def append_edge(self, edge: EdgeT, weight: int) -> None:
        """Add an edge with the given weight to this path."""
        tail_node = edge.tail_node
        head_node = edge.head_node
        if len(self._nodes) == 0:
            self._append_node(tail_node)
        self._edge_addition_callback(edge)
        self._append_node(head_node)
        self._edges.append(edge)
        self._weight_addition_order.append(weight)
        self._current_weight += weight

    @property
    def weights(self) -> Sequence[int]:
        """The sequence of weights that were added to compute the total weight of the path."""
        return self._weight_addition_order

    @property
    def weight(self) -> int:
        """The current weight of the path."""
        return self._current_weight

    def pop(self) -> None:
        """Remove the last node / edge added to the path."""
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
    def node_set(self) -> MutableOrderedSet[NodeT]:
        """A set containing the nodes in this path."""
        return self._current_node_set


MutablePathT = TypeVar("MutablePathT", bound=MutableGraphPath)
