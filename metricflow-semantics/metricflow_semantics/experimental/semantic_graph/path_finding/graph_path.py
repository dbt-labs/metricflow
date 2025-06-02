from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import ClassVar, Generic, Optional, Sequence, TypeVar, override

from typing_extensions import override

from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraphEdge, MetricflowGraphNode
from metricflow_semantics.experimental.ordered_set import MutableOrderedSet, OrderedSet
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.mf_logging.pretty_formatter import PrettyFormatContext

logger = logging.getLogger(__name__)


NodeT = TypeVar("NodeT", bound=MetricflowGraphNode)
EdgeT = TypeVar("EdgeT", bound=MetricflowGraphEdge)


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

    @property
    @abstractmethod
    def node_set(self) -> OrderedSet[NodeT]:
        raise NotImplementedError()


@dataclass
class MutableGraphPath(MetricflowGraphPath[NodeT, EdgeT]):
    _nodes: list[NodeT]
    _edges: list[EdgeT]
    _weight_addition_order: list[int]
    _current_weight: int
    _current_node_set: MutableOrderedSet[NodeT]
    _node_set_addition_order: list[Optional[NodeT]]

    _verbose_debug_logs: ClassVar[bool] = True

    @staticmethod
    def create() -> MutableGraphPath:
        return MutableGraphPath(
            _nodes=[],
            _edges=[],
            _weight_addition_order=[],
            _current_weight=0,
            _current_node_set=MutableOrderedSet(),
            _node_set_addition_order=[],
        )

    def reset_to_start_node(self, start_node: NodeT) -> None:
        self._nodes.clear()
        self._edges.clear()
        self._weight_addition_order.clear()
        self._current_weight = 0
        self._current_node_set.clear()
        self._node_set_addition_order.clear()

        self._append_node(start_node)

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
        self._node_addition_callback(node)

    def _node_addition_callback(self, node: NodeT) -> None:
        pass

    def _edge_addition_callback(self, edge: EdgeT) -> None:
        pass

    def append_edge(self, edge: EdgeT, weight: int) -> None:
        tail_node = edge.tail_node
        head_node = edge.head_node
        if len(self._nodes) == 0:
            self._append_node(tail_node)
        self._append_node(head_node)
        self._edges.append(edge)
        self._weight_addition_order.append(weight)
        self._current_weight += weight
        self._edge_addition_callback(edge)

    @property
    def weights(self) -> Sequence[int]:
        return self._weight_addition_order

    @property
    def weight(self) -> int:
        return self._current_weight

    def pop(self) -> None:
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
        return self._current_node_set


PathT = TypeVar(
    "PathT",
    bound=MutableGraphPath,
)
