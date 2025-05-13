from __future__ import annotations

import logging
from abc import ABC
from collections import defaultdict
from dataclasses import dataclass
from typing import DefaultDict, Generic, TypeVar

from metricflow_semantics.experimental.mf_graph.displayable_graph_element import MetricflowGraphProperty
from metricflow_semantics.experimental.mf_graph.mf_graph import EdgeT, MetricflowGraph, MetricflowGraphEdge, NodeT
from metricflow_semantics.experimental.ordered_set import MutableOrderedSet, OrderedSet

logger = logging.getLogger(__name__)

MutableGraphT = TypeVar("MutableGraphT", bound="MutableGraph")


@dataclass
class MutableGraph(Generic[NodeT, EdgeT], MetricflowGraph[NodeT, EdgeT], ABC):
    _nodes: MutableOrderedSet[NodeT]
    _edges: MutableOrderedSet[EdgeT]

    _property_to_nodes: DefaultDict[MetricflowGraphProperty, MutableOrderedSet[NodeT]]
    _tail_node_to_edges: DefaultDict[NodeT, MutableOrderedSet[EdgeT]]

    @classmethod
    def create(cls) -> MutableGraph[NodeT, EdgeT]:
        return cls(
            _nodes=MutableOrderedSet(),
            _edges=MutableOrderedSet(),
            _property_to_nodes=defaultdict(MutableOrderedSet),
            _tail_node_to_edges=defaultdict(MutableOrderedSet),
        )

    def add_node(self, node: NodeT) -> None:
        self._nodes.add(node)

    def add_edge(self, edge: MetricflowGraphEdge[NodeT]) -> None:
        self.add_node(edge.tail_node)

    def nodes(self) -> OrderedSet[NodeT]:
        return self._nodes

    def nodes_with_property(self, graph_property: MetricflowGraphProperty) -> OrderedSet[NodeT]:
        return self._property_to_nodes[graph_property]

    def edges(self) -> OrderedSet[EdgeT]:
        return self._edges

    def edges_with_tail_node(self, tail_node: NodeT) -> OrderedSet[EdgeT]:
        return self._tail_node_to_edges[tail_node]
