from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence, Set
from dataclasses import dataclass
from typing import Generic, Optional, Sized, TypeVar

from typing_extensions import Self, override

from metricflow_semantics.toolkit.mf_graph.comparable import Comparable, ComparisonKey
from metricflow_semantics.toolkit.mf_graph.mf_graph import MetricFlowGraphEdge, MetricFlowGraphNode
from metricflow_semantics.toolkit.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import PrettyFormatContext
from metricflow_semantics.toolkit.mf_logging.pretty_print import mf_pformat

logger = logging.getLogger(__name__)


NodeT = TypeVar("NodeT", bound=MetricFlowGraphNode)
EdgeT = TypeVar("EdgeT", bound=MetricFlowGraphEdge)


class MetricFlowGraphPath(Generic[NodeT, EdgeT], Comparable, MetricFlowPrettyFormattable, Sized, ABC):
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
    def weight(self) -> int:
        """The weight of this path as defined by the weight function that was used during traversal."""
        raise NotImplementedError

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
    def node_set(self) -> Set[NodeT]:
        """The set of nodes in this path. Useful for fast checks for cycles during path extension."""
        raise NotImplementedError()

    @override
    def __len__(self) -> int:
        return len(self.nodes)

    @abstractmethod
    def copy(self) -> Self:
        """Return a shallow copy of this path."""
        raise NotImplementedError

    def arrow_format(self) -> str:
        """Return a string representation that uses `->` between nodes.

        TODO: This is only used in tests so it should be migrated elsewhere.
        """
        return f"[weight: {self.weight}] " + " -> ".join([mf_pformat(node) for node in self.nodes])


@dataclass
class MutableGraphPath(MetricFlowGraphPath[NodeT, EdgeT], Generic[NodeT, EdgeT]):
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
    _current_node_set: set[NodeT]

    # As this path is extended step-by-step, keep track of the weights added so that when `pop()` is called, the weight
    # of the path afterward can be easily computed by subtracting the last incremental weight added. Similar situation
    # for `_node_set_addition_order`.
    _weight_addition_order: list[int]
    _node_set_addition_order: list[Optional[NodeT]]

    @staticmethod
    def create(start_node: Optional[NodeT] = None) -> MutableGraphPath:  # noqa: D102
        path: MutableGraphPath[NodeT, EdgeT] = MutableGraphPath(
            _nodes=[],
            _edges=[],
            _weight_addition_order=[],
            _current_weight=0,
            _current_node_set=set(),
            _node_set_addition_order=[],
        )
        if start_node:
            path._append_node(start_node)
        return path

    @property
    def edges(self) -> Sequence[EdgeT]:  # noqa: D102
        return self._edges

    @property
    def nodes(self) -> Sequence[NodeT]:  # noqa: D102
        return self._nodes

    @property
    def is_empty(self) -> bool:  # noqa: D102
        return not self._nodes

    def _append_node(self, node: NodeT) -> None:
        """Helper to add a node to the path."""
        self._nodes.append(node)
        if node in self._current_node_set:
            self._node_set_addition_order.append(None)
        else:
            self._current_node_set.add(node)
            self._node_set_addition_order.append(node)

    def append_edge(self, edge: EdgeT, weight: int) -> None:
        """Add an edge with the given weight to this path."""
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
        """The sequence of weights that were added to compute the total weight of the path."""
        return self._weight_addition_order

    @property
    def weight(self) -> int:
        """The current weight of the path."""
        return self._current_weight

    def pop_end(self) -> None:
        """Remove the last node / edge added to the path."""
        if not self._edges:
            if not self._nodes:
                raise KeyError("Can't pop an empty path")
            self._nodes.pop()
            return

        self._edges.pop()
        self._nodes.pop()
        weight = self._weight_addition_order.pop()
        self._current_weight -= weight

        added_node = self._node_set_addition_order.pop()
        if added_node is not None:
            self._current_node_set.remove(added_node)
        return

    @property
    def node_set(self) -> Set[NodeT]:
        """A set containing the nodes in this path."""
        return self._current_node_set

    @override
    def copy(self) -> Self:
        # noinspection PyArgumentList
        return self.__class__(
            _nodes=self._nodes.copy(),
            _edges=self._edges.copy(),
            _current_weight=self._current_weight,
            _current_node_set=self._current_node_set.copy(),
            _weight_addition_order=self._weight_addition_order.copy(),
            _node_set_addition_order=self._node_set_addition_order.copy(),
        )

    @property
    @override
    def comparison_key(self) -> ComparisonKey:
        return (
            tuple(self._nodes),
            self._current_weight,
            tuple(self._edges),
        )


MutablePathT = TypeVar("MutablePathT", bound=MutableGraphPath)
