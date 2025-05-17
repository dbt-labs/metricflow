from __future__ import annotations

import logging
from abc import ABC
from collections import defaultdict
from dataclasses import dataclass
from functools import cached_property
from typing import Iterable

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
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
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_str_snapshot_equal
from typing_extensions import override

from tests_metricflow_semantics.experimental.graph_helpers import assert_graph_snapshot_equal
from tests_metricflow_semantics.experimental.mf_graph.formatting.dot_formatter import DotNotationFormatter
from tests_metricflow_semantics.experimental.mf_graph.formatting.svg_formatter import SvgFormatter

logger = logging.getLogger(__name__)


# The `Flow*` classes are an example implementation of the graph classes used for tests in this module.


@singleton_dataclass(order=False)
class FlowNode(MetricflowGraphNode, ABC):  # noqa: D101
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
class FlowEdge(MetricflowGraphEdge):  # noqa: D101
    @staticmethod
    def get_instance(tail_node: FlowNode, head_node: FlowNode) -> FlowEdge:
        return FlowEdge(
            _tail_node=tail_node,
            _head_node=head_node,
            _weight=1,
        )

    @property
    @override
    def tail_node(self) -> FlowNode:
        return self._tail_node

    @property
    @override
    def head_node(self) -> FlowNode:
        return self._head_node

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return self._tail_node, self._head_node

    @override
    @property
    def inverse(self) -> FlowEdge:
        return FlowEdge.get_instance(
            tail_node=self._head_node,
            head_node=self._tail_node,
        )


@dataclass
class FlowGraph(MutableGraph[FlowNode, FlowEdge], MetricFlowPrettyFormattable):  # noqa: D101
    @staticmethod
    def create(nodes: Iterable[FlowNode] = (), edges: Iterable[FlowEdge] = ()) -> FlowGraph:
        graph = FlowGraph(
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

    def inverse(self) -> FlowGraph:
        return FlowGraph.create(edges=(edge.inverse for edge in self.edges))


@pytest.fixture(scope="session")
def flow_graph() -> FlowGraph:
    """Example instance of a graph used in test cases."""
    source_node = SourceNode(node_name="source")
    a_node = IntermediateNode(node_name="a")
    b_node = IntermediateNode(node_name="b")
    sink_node = SinkNode(node_name="sink")

    return FlowGraph.create(
        nodes=MutableOrderedSet((source_node, a_node, b_node, sink_node)),
        edges=MutableOrderedSet(
            (
                FlowEdge.get_instance(tail_node=source_node, head_node=a_node),
                FlowEdge.get_instance(tail_node=source_node, head_node=b_node),
                FlowEdge.get_instance(tail_node=a_node, head_node=sink_node),
                FlowEdge.get_instance(tail_node=b_node, head_node=sink_node),
            )
        ),
    )


def test_default_format(
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, flow_graph: FlowGraph
) -> None:
    """Check formatting of the graph using `pretty_format`."""
    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_str=flow_graph.format(),
    )


def test_dot_notation_format(
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, flow_graph: FlowGraph
) -> None:
    """Check formatting of the graph using `dot_notation`."""
    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_str=flow_graph.format(DotNotationFormatter()),
    )


def test_svg_format(
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, flow_graph: FlowGraph
) -> None:
    """Check formatting of the graph using `dot_notation`."""
    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_str=flow_graph.format(SvgFormatter()),
    )


def test_graph_snapshot(
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, flow_graph: FlowGraph
) -> None:
    """Check the graph snapshot."""
    assert_graph_snapshot_equal(request=request, snapshot_configuration=mf_test_configuration, graph=flow_graph)
