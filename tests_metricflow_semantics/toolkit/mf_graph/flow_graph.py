"""`Flow*` classes are an example implementation of a graph used in test cases."""
from __future__ import annotations

import logging
from abc import ABC
from collections import defaultdict
from dataclasses import dataclass
from functools import cached_property
from typing import Iterable

from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.toolkit.collections.ordered_set import MutableOrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_graph.comparable import ComparisonKey
from metricflow_semantics.toolkit.mf_graph.graph_id import SequentialGraphId
from metricflow_semantics.toolkit.mf_graph.mf_graph import (
    MetricFlowGraph,
    MetricFlowGraphEdge,
    MetricFlowGraphNode,
)
from metricflow_semantics.toolkit.mf_graph.mutable_graph import MutableGraph
from metricflow_semantics.toolkit.mf_graph.node_descriptor import MetricFlowGraphNodeDescriptor
from metricflow_semantics.toolkit.mf_graph.path_finding.graph_path import MutableGraphPath
from metricflow_semantics.toolkit.mf_graph.path_finding.pathfinder import MetricFlowPathfinder
from metricflow_semantics.toolkit.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.toolkit.singleton import Singleton
from typing_extensions import override

logger = logging.getLogger(__name__)


@fast_frozen_dataclass(order=False)
class FlowNode(MetricFlowGraphNode, ABC):
    """Example graph node."""

    node_name: str

    @override
    @property
    def comparison_key(self) -> ComparisonKey:
        return (self.node_name,)

    @override
    @property
    def node_descriptor(self) -> MetricFlowGraphNodeDescriptor:
        return MetricFlowGraphNodeDescriptor(node_name=self.node_name, cluster_name=None)


@fast_frozen_dataclass(order=False)
class SourceNode(FlowNode, Singleton):  # noqa: D101
    @classmethod
    def get_instance(cls, node_name: str) -> SourceNode:  # noqa: D102
        return cls._get_instance(node_name=node_name)


@fast_frozen_dataclass(order=False)
class SinkNode(FlowNode, Singleton):  # noqa: D101
    @classmethod
    def get_instance(cls, node_name: str) -> SinkNode:  # noqa: D102
        return cls._get_instance(node_name=node_name)


@fast_frozen_dataclass(order=False)
class IntermediateNode(FlowNode, Singleton):  # noqa: D101
    @classmethod
    def get_instance(cls, node_name: str) -> IntermediateNode:  # noqa: D102
        return cls._get_instance(node_name=node_name)

    @override
    @property
    def node_descriptor(self) -> MetricFlowGraphNodeDescriptor:
        return MetricFlowGraphNodeDescriptor(node_name=self.node_name, cluster_name="intermediate_nodes")


@fast_frozen_dataclass(order=False)
class FlowEdge(MetricFlowGraphEdge):
    """Example graph edge."""

    weight: int

    @override
    @cached_property
    def comparison_key(self) -> ComparisonKey:
        return self.tail_node, self.head_node

    @property
    @override
    def inverse(self) -> FlowEdge:
        return FlowEdge(
            tail_node=self.head_node,
            head_node=self.tail_node,
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
            _node_to_successor_nodes=defaultdict(MutableOrderedSet),
            _node_to_predecessor_nodes=defaultdict(MutableOrderedSet),
        )
        for node in nodes:
            graph.add_node(node)
        for edge in edges:
            graph.add_edge(edge)
        return graph

    @override
    def intersection(self, other: MetricFlowGraph[FlowNode, FlowEdge]) -> FlowGraph:
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


FlowGraphPath = MutableGraphPath[FlowNode, FlowEdge]
FlowGraphPathFinder = MetricFlowPathfinder[FlowNode, FlowEdge, FlowGraphPath]
