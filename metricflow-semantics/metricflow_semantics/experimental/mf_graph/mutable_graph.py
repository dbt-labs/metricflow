from __future__ import annotations

import logging
from abc import ABC
from dataclasses import dataclass
from typing import DefaultDict, Generic, Iterable, TypeVar

from typing_extensions import override

from metricflow_semantics.experimental.mf_graph.displayable_graph_element import MetricflowGraphProperty
from metricflow_semantics.experimental.mf_graph.mf_graph import EdgeT, MetricflowGraph, NodeT
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet

logger = logging.getLogger(__name__)

MutableGraphT = TypeVar("MutableGraphT", bound="MutableGraph")


@dataclass
class MutableGraph(Generic[NodeT, EdgeT], MetricflowGraph[NodeT, EdgeT], ABC):
    _nodes: MutableOrderedSet[NodeT]
    _edges: MutableOrderedSet[EdgeT]

    _node_property_to_nodes: DefaultDict[MetricflowGraphProperty, MutableOrderedSet[NodeT]]
    _tail_node_to_edges: DefaultDict[NodeT, MutableOrderedSet[EdgeT]]
    _head_node_to_edges: DefaultDict[NodeT, MutableOrderedSet[EdgeT]]

    def add_node(self, node: NodeT) -> None:
        self._nodes.add(node)
        for node_property in node.properties:
            self._node_property_to_nodes[node_property].add(node)

    def add_nodes(self, nodes: Iterable[NodeT]) -> None:
        for node in nodes:
            self.add_node(node)

    def add_edge(self, edge: EdgeT) -> None:
        self.add_node(edge.tail_node)
        self.add_node(edge.head_node)
        self._tail_node_to_edges[edge.tail_node].add(edge)
        self._head_node_to_edges[edge.head_node].add(edge)
        self._edges.add(edge)

    def add_edges(self, edges: Iterable[EdgeT]) -> None:
        for edge in edges:
            self.add_edge(edge)

    def nodes(self) -> OrderedSet[NodeT]:
        return self._nodes

    def nodes_with_property(self, graph_property: MetricflowGraphProperty) -> OrderedSet[NodeT]:
        return self._node_property_to_nodes[graph_property]

    def edges(self) -> OrderedSet[EdgeT]:
        return self._edges

    def edges_with_tail_node(self, tail_node: NodeT) -> OrderedSet[EdgeT]:
        return self._tail_node_to_edges[tail_node]

    @property
    @override
    def properties(self) -> OrderedSet[MetricflowGraphProperty]:
        return FrozenOrderedSet()
