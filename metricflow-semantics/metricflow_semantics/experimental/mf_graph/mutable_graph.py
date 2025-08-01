from __future__ import annotations

import logging
from abc import ABC
from dataclasses import dataclass
from typing import DefaultDict, Generic, Iterable, TypeVar

from typing_extensions import override

from metricflow_semantics.collection_helpers.syntactic_sugar import mf_flatten
from metricflow_semantics.experimental.mf_graph.graph_id import MetricflowGraphId, SequentialGraphId
from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.mf_graph.mf_graph import (
    EdgeT,
    MetricflowGraph,
    MetricflowGraphNode,
    NodeT,
)
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet

logger = logging.getLogger(__name__)

MutableGraphT = TypeVar("MutableGraphT", bound="MutableGraph")


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

    _node_to_predecessor_nodes: DefaultDict[MetricflowGraphNode, MutableOrderedSet[NodeT]]
    _node_to_successor_nodes: DefaultDict[MetricflowGraphNode, MutableOrderedSet[NodeT]]

    def add_node(self, node: NodeT) -> None:  # noqa: D102
        self.add_nodes((node,))

    def add_nodes(self, nodes: Iterable[NodeT]) -> None:  # noqa: D102
        self._nodes.update(nodes)
        for node in nodes:
            for node_label in node.labels:
                self._label_to_nodes[node_label].add(node)
        self._graph_id = SequentialGraphId.create()

    def add_edge(self, edge: EdgeT) -> None:  # noqa: D102
        self.add_edges((edge,))

    def add_edges(self, edges: Iterable[EdgeT]) -> None:  # noqa: D102
        tail_nodes = [edge.tail_node for edge in edges]
        head_nodes = [edge.head_node for edge in edges]

        nodes_to_add: MutableOrderedSet[NodeT] = MutableOrderedSet()
        nodes_to_add.update(tail_nodes, head_nodes)
        nodes_to_add.difference_update(self.nodes)
        self.add_nodes(nodes_to_add)

        for edge in edges:
            tail_node = edge.tail_node
            head_node = edge.head_node

            self._tail_node_to_edges[tail_node].add(edge)
            self._head_node_to_edges[head_node].add(edge)
            self._node_to_successor_nodes[tail_node].add(head_node)
            self._node_to_predecessor_nodes[head_node].add(tail_node)

        self._edges.update(edges)
        self._graph_id = SequentialGraphId.create()

    def update(self, other: MetricflowGraph[NodeT, EdgeT]) -> None:
        """Add the nodes and edges to this graph."""
        self.add_nodes(other.nodes)
        self.add_edges(other.edges)
        self._graph_id = SequentialGraphId.create()

    @override
    @property
    def nodes(self) -> OrderedSet[NodeT]:  # noqa: D102
        return self._nodes

    @override
    def nodes_with_labels(self, *graph_labels: MetricflowGraphLabel) -> OrderedSet[NodeT]:
        return FrozenOrderedSet(mf_flatten(self._label_to_nodes[label] for label in graph_labels))

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
        return self._node_to_successor_nodes[node]

    @override
    def predecessors(self, node: MetricflowGraphNode) -> OrderedSet[NodeT]:
        return self._node_to_predecessor_nodes[node]

    @override
    @property
    def graph_id(self) -> MetricflowGraphId:
        return self._graph_id
