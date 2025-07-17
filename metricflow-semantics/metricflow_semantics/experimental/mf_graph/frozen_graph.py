from __future__ import annotations

import logging
from abc import ABC
from collections import defaultdict
from functools import cached_property
from typing import DefaultDict, Generic, Mapping, TypeVar

from typing_extensions import Self, override

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.graph_id import MetricflowGraphId
from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.mf_graph.mf_graph import (
    MetricflowGraph,
    MetricflowGraphEdge,
    MetricflowGraphNode,
)
from metricflow_semantics.experimental.mf_graph.mutable_graph import MutableGraph
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet

logger = logging.getLogger(__name__)

MutableGraphT = TypeVar("MutableGraphT", bound="MutableGraph")

EdgeT = TypeVar("EdgeT", bound=MetricflowGraphEdge, covariant=True)
NodeT = TypeVar("NodeT", bound=MetricflowGraphNode, covariant=True)


@fast_frozen_dataclass()
class FrozenGraph(Generic[NodeT, EdgeT], MetricflowGraph[NodeT, EdgeT], ABC):
    """Base class for mutable graphs."""

    graph_id: MetricflowGraphId
    _nodes: FrozenOrderedSet[NodeT]
    _edges: FrozenOrderedSet[EdgeT]

    @staticmethod
    def create(
        graph_id: MetricflowGraphId, nodes: OrderedSet[NodeT], edges: OrderedSet[EdgeT]
    ) -> FrozenGraph[NodeT, EdgeT]:
        return FrozenGraph(
            graph_id=graph_id,
            _nodes=nodes.as_frozen(),
            _edges=edges.as_frozen(),
        )

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

    def as_sorted(self) -> FrozenGraph[NodeT, EdgeT]:
        return FrozenGraph(
            graph_id=self.graph_id,
            _nodes=FrozenOrderedSet(sorted(self._nodes)),
            _edges=FrozenOrderedSet(sorted(self._edges)),
        )

    @override
    def adjacent_edges(self, selected_nodes: OrderedSet[NodeT]) -> FrozenOrderedSet[EdgeT]:
        subgraph_edges = MutableOrderedSet[EdgeT]()
        for node in selected_nodes:
            for edge in self.edges_with_tail_node(node):
                if edge.head_node in selected_nodes:
                    subgraph_edges.add(edge)
            for edge in self.edges_with_head_node(node):
                if edge.tail_node in selected_nodes:
                    subgraph_edges.add(edge)
        return subgraph_edges.as_frozen()

    @cached_property
    def _label_to_nodes(self) -> Mapping[MetricflowGraphLabel, FrozenOrderedSet[NodeT]]:
        label_to_nodes: DefaultDict[MetricflowGraphLabel, MutableOrderedSet[NodeT]] = defaultdict(
            MutableOrderedSet[NodeT]
        )
        for node in self._nodes:
            for label in node.labels:
                label_to_nodes[label].add(node)

        return {label: nodes.as_frozen() for label, nodes in label_to_nodes.items()}

    @cached_property
    def _label_to_edges(self) -> Mapping[MetricflowGraphLabel, FrozenOrderedSet[EdgeT]]:
        label_to_edges: DefaultDict[MetricflowGraphLabel, MutableOrderedSet[EdgeT]] = defaultdict(
            MutableOrderedSet[EdgeT]
        )
        for edge in self._edges:
            for label in edge.labels:
                label_to_edges[label].add(edge)
        return {label: edges.as_frozen() for label, edges in label_to_edges.items()}

    @cached_property
    def _tail_node_to_edges(self) -> Mapping[MetricflowGraphNode, FrozenOrderedSet[EdgeT]]:
        tail_node_to_edges: DefaultDict[NodeT, MutableOrderedSet[EdgeT]] = defaultdict(MutableOrderedSet[EdgeT])
        for edge in self._edges:
            tail_node_to_edges[edge.tail_node].add(edge)
        return {tail_node: FrozenOrderedSet(edges) for tail_node, edges in tail_node_to_edges.items()}

    @cached_property
    def _head_node_to_edges(self) -> Mapping[MetricflowGraphNode, FrozenOrderedSet[EdgeT]]:
        head_node_to_edges: DefaultDict[NodeT, MutableOrderedSet[EdgeT]] = defaultdict(MutableOrderedSet[EdgeT])
        for edge in self._edges:
            head_node_to_edges[edge.head_node].add(edge)
        return {head_node: FrozenOrderedSet(edges) for head_node, edges in head_node_to_edges.items()}

    @override
    def intersection(self, other: MetricflowGraph[NodeT, EdgeT]) -> Self:
        raise NotImplementedError()

    @override
    def inverse(self) -> Self:
        raise NotImplementedError()
