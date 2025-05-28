from __future__ import annotations

import logging
from abc import ABC
from collections import defaultdict
from dataclasses import dataclass
from typing import DefaultDict, Generic, Iterable, TypeVar

from typing_extensions import Self, override

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
    """Base class for mutable graphs."""

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

    def add_nodes(self, nodes: Iterable[NodeT]) -> None:  # noqa: D102
        for node in nodes:
            self.add_node(node)

    def add_edge(self, edge: EdgeT) -> None:  # noqa: D102
        self.add_node(edge.tail_node)
        self.add_node(edge.head_node)
        self._tail_node_to_edges[edge.tail_node].add(edge)
        self._head_node_to_edges[edge.head_node].add(edge)
        self._edges.add(edge)

    def add_edges(self, edges: Iterable[EdgeT]) -> None:  # noqa: D102
        for edge in edges:
            self.add_edge(edge)

    def update(self, other: MetricflowGraph[NodeT, EdgeT]) -> None:  # noqa: D102
        self.add_edges(other.edges)

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

    def as_sorted(self) -> Self:
        """Return this graph but with nodes and edges sorted."""
        # noinspection PyArgumentList
        updated_graph = self.__class__(
            _nodes=MutableOrderedSet(),
            _edges=MutableOrderedSet(),
            _tail_node_to_edges=defaultdict(MutableOrderedSet),
            _head_node_to_edges=defaultdict(MutableOrderedSet),
            _label_to_nodes=defaultdict(MutableOrderedSet),
            _label_to_edges=defaultdict(MutableOrderedSet),
        )
        for node in sorted(self._nodes):
            updated_graph.add_node(node)

        for edge in sorted(self._edges):
            updated_graph.add_edge(edge)

        return updated_graph

    @override
    def adjacent_edges(self, selected_nodes: OrderedSet[NodeT]) -> MutableOrderedSet[EdgeT]:
        subgraph_edges = MutableOrderedSet[EdgeT]()
        for node in selected_nodes:
            for edge in self.edges_with_tail_node(node):
                if edge.head_node in selected_nodes:
                    subgraph_edges.add(edge)
            for edge in self.edges_with_head_node(node):
                if edge.tail_node in selected_nodes:
                    subgraph_edges.add(edge)
        return subgraph_edges
