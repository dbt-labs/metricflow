from __future__ import annotations

import logging
from abc import ABC
from dataclasses import dataclass
from typing import DefaultDict, Generic, Iterable, TypeVar

from typing_extensions import override

from metricflow_semantics.experimental.mf_graph.graph_id import MetricflowGraphId, SequentialGraphId
from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.mf_graph.mf_graph import (
    MetricflowGraph,
    MetricflowGraphEdge,
    MetricflowGraphNode,
)
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet

logger = logging.getLogger(__name__)

MutableGraphT = TypeVar("MutableGraphT", bound="MutableGraph")

EdgeT = TypeVar("EdgeT", bound=MetricflowGraphEdge)
NodeT = TypeVar("NodeT", bound=MetricflowGraphNode)


@dataclass
class MutableGraph(Generic[NodeT, EdgeT], MetricflowGraph[NodeT, EdgeT], ABC):
    """Base class for mutable graphs.

    The graph ID is changed to a new value whenever this changes for easier cache management.
    """

    _graph_id: MetricflowGraphId
    _nodes: MutableOrderedSet[NodeT]
    _edges: MutableOrderedSet[EdgeT]

    _label_to_nodes: DefaultDict[MetricflowGraphLabel, MutableOrderedSet[NodeT]]
    _tail_node_to_edges: DefaultDict[MetricflowGraphNode, MutableOrderedSet[EdgeT]]
    _head_node_to_edges: DefaultDict[MetricflowGraphNode, MutableOrderedSet[EdgeT]]
    _label_to_edges: DefaultDict[MetricflowGraphLabel, MutableOrderedSet[EdgeT]]

    def add_node(self, node: NodeT) -> None:  # noqa: D102
        self._nodes.add(node)
        for node_property in node.labels:
            self._label_to_nodes[node_property].add(node)
        self._graph_id = SequentialGraphId.create()

    def add_nodes(self, nodes: Iterable[NodeT]) -> None:  # noqa: D102
        for node in nodes:
            self.add_node(node)

    def add_edge(self, edge: EdgeT) -> None:  # noqa: D102
        tail_node = edge.tail_node
        head_node = edge.head_node
        graph_nodes = self._nodes

        if tail_node not in graph_nodes:
            self.add_node(tail_node)
        if head_node not in graph_nodes:
            self.add_node(head_node)

        self._tail_node_to_edges[tail_node].add(edge)
        self._head_node_to_edges[head_node].add(edge)
        self._edges.add(edge)
        self._graph_id = SequentialGraphId.create()

    def add_edges(self, edges: Iterable[EdgeT]) -> None:  # noqa: D102
        for edge in edges:
            self.add_edge(edge)

    def update(self, other: MetricflowGraph[NodeT, EdgeT]) -> None:
        """Add the nodes and edges to this graph."""
        if len(other.nodes) == 0 and len(other.edges) == 0:
            return

        self.add_nodes(other.nodes)
        self.add_edges(other.edges)
        self._graph_id = SequentialGraphId.create()

    @override
    @property
    def nodes(self) -> OrderedSet[NodeT]:  # noqa: D102
        return self._nodes

    @override
    def nodes_with_label(self, graph_label: MetricflowGraphLabel) -> OrderedSet[NodeT]:
        return self._label_to_nodes[graph_label]

    @override
    @property
    def edges(self) -> OrderedSet[EdgeT]:
        return self._edges

    @override
    def edges_with_tail_node(self, tail_node: MetricflowGraphNode) -> OrderedSet[EdgeT]:
        return self._tail_node_to_edges[tail_node]

    @override
    def edges_with_head_node(self, head_node: MetricflowGraphNode) -> OrderedSet[EdgeT]:
        return self._head_node_to_edges[head_node]

    @override
    def edges_with_label(self, label: MetricflowGraphLabel) -> OrderedSet[EdgeT]:
        return self._label_to_edges[label]

    @override
    def successors(self, node: MetricflowGraphNode) -> OrderedSet[NodeT]:
        return FrozenOrderedSet(edge.head_node for edge in self.edges_with_tail_node(node))

    @override
    def predecessors(self, node: MetricflowGraphNode) -> OrderedSet[NodeT]:
        return FrozenOrderedSet(edge.tail_node for edge in self.edges_with_head_node(node))

    @override
    @property
    def graph_id(self) -> MetricflowGraphId:
        return self._graph_id
