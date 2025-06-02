"""`Flow*` classes are an example implementation of a graph used in test cases."""
from __future__ import annotations

import logging
from abc import ABC
from collections import defaultdict
from dataclasses import dataclass
from functools import cached_property
from typing import Iterable

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
from metricflow_semantics.experimental.mf_graph.graph_id import SequentialGraphId
from metricflow_semantics.experimental.mf_graph.mf_graph import (
    MetricflowGraph,
    MetricflowGraphEdge,
    MetricflowGraphNode,
)
from metricflow_semantics.experimental.mf_graph.mutable_graph import MutableGraph
from metricflow_semantics.experimental.mf_graph.node_descriptor import MetricflowGraphNodeDescriptor
from metricflow_semantics.experimental.ordered_set import MutableOrderedSet
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass
from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from typing_extensions import override

logger = logging.getLogger(__name__)


@singleton_dataclass(order=False)
class FlowNode(MetricflowGraphNode, ABC):
    """Example graph node."""

    node_name: str

    @override
    @property
    def comparison_key(self) -> ComparisonKey:
        return (self.node_name,)

    @override
    @property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(node_name=self.node_name, cluster_name=None)


@singleton_dataclass(order=False)
class SourceNode(FlowNode):  # noqa: D101
    pass


@singleton_dataclass(order=False)
class SinkNode(FlowNode):  # noqa: D101
    pass


@singleton_dataclass(order=False)
class IntermediateNode(FlowNode):  # noqa: D101
    @override
    @property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(node_name=self.node_name, cluster_name="intermediate_nodes")


@singleton_dataclass(order=False)
class FlowEdge(MetricflowGraphEdge):
    """Example graph edge."""

    weight: int

    @staticmethod
    def get_instance(tail_node: FlowNode, head_node: FlowNode, weight: int = 1) -> FlowEdge:  # noqa: D102
        return FlowEdge(
            _tail_node=tail_node,
            _head_node=head_node,
            weight=weight,
        )

    @override
    @property
    def tail_node(self) -> FlowNode:
        return self._tail_node

    @override
    @property
    def head_node(self) -> FlowNode:
        return self._head_node

    @override
    @cached_property
    def comparison_key(self) -> ComparisonKey:
        return self._tail_node, self._head_node

    @property
    @override
    def inverse(self) -> FlowEdge:
        return FlowEdge.get_instance(
            tail_node=self._head_node,
            head_node=self._tail_node,
            weight=self.weight,
        )

    @override
    @cached_property
    def displayed_properties(self) -> AnyLengthTuple[DisplayedProperty]:
        return (DisplayedProperty("weight", self.weight),)


@dataclass
class FlowGraph(MutableGraph[FlowNode, FlowEdge], MetricFlowPrettyFormattable):
    """Example graph."""

    @classmethod
    def create(cls, nodes: Iterable[FlowNode] = (), edges: Iterable[FlowEdge] = ()) -> FlowGraph:  # noqa: D102
        graph = FlowGraph(
            _graph_id=SequentialGraphId.create(),
            _nodes=MutableOrderedSet(),
            _edges=MutableOrderedSet(),
            _label_to_nodes=defaultdict(MutableOrderedSet),
            _tail_node_to_edges=defaultdict(MutableOrderedSet),
            _head_node_to_edges=defaultdict(MutableOrderedSet),
            _label_to_edges=defaultdict(MutableOrderedSet),
        )
        for node in nodes:
            graph.add_node(node)
        for edge in edges:
            graph.add_edge(edge)
        return graph

    @override
    def intersection(self, other: MetricflowGraph[FlowNode, FlowEdge]) -> FlowGraph:
        intersection_graph = FlowGraph.create()
        self.add_edges(self._intersect_edges(other))
        return intersection_graph

    @override
    def inverse(self) -> FlowGraph:
        return FlowGraph.create(edges=(edge.inverse for edge in self.edges))

    @override
    def as_sorted(self) -> FlowGraph:
        """Return this graph but with nodes and edges sorted."""
        # noinspection PyArgumentList
        updated_graph = FlowGraph.create()
        for node in sorted(self._nodes):
            updated_graph.add_node(node)

        for edge in sorted(self._edges):
            updated_graph.add_edge(edge)

        return updated_graph
