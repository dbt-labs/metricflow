from __future__ import annotations

import logging
from abc import ABC
from collections import defaultdict
from dataclasses import dataclass
from functools import cached_property
from typing import Iterable, Optional

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
from metricflow_semantics.experimental.mf_graph.mf_graph import (
    MetricflowGraphEdge,
    MetricflowGraphNode,
)
from metricflow_semantics.experimental.mf_graph.mutable_graph import MutableGraph
from metricflow_semantics.experimental.ordered_set import MutableOrderedSet, OrderedSet
from metricflow_semantics.experimental.singleton import Singleton
from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.mf_logging.pretty_formatter import PrettyFormatContext
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_object_snapshot_equal, assert_str_snapshot_equal
from typing_extensions import override

from tests_metricflow_semantics.experimental.graph_helpers import assert_graph_snapshot_equal

logger = logging.getLogger(__name__)


# The `Flow*` classes are an example implementation of the graph classes used for tests in this module.


@fast_frozen_dataclass(order=False)
class FlowNode(MetricflowGraphNode, Singleton, ABC):  # noqa: D101
    node_name: str

    @property
    @override
    def dot_label(self) -> str:
        return self.node_name

    @property
    @override
    def graphviz_label(self) -> str:
        return self.node_name

    @property
    @override
    def comparison_key(self) -> ComparisonKey:
        return (self.node_name,)

    @classmethod
    def get_instance(cls, node_name: str) -> FlowNode:  # noqa: D102
        return cls._get_singleton_by_kwargs(node_name=node_name)


@fast_frozen_dataclass(order=False)
class SourceNode(FlowNode):  # noqa: D101
    pass


@fast_frozen_dataclass(order=False)
class SinkNode(FlowNode):  # noqa: D101
    pass


@fast_frozen_dataclass(order=False)
class IntermediateNode(FlowNode):  # noqa: D101
    pass


@fast_frozen_dataclass(order=False)
class FlowEdge(MetricflowGraphEdge, Singleton):  # noqa: D101
    edge_name: str
    _tail_node: FlowNode
    _head_node: FlowNode

    @property
    @override
    def tail_node(self) -> FlowNode:
        return self._tail_node

    @property
    @override
    def head_node(self) -> FlowNode:
        return self._head_node

    @property
    @override
    def dot_label(self) -> str:
        return self.edge_name

    @property
    @override
    def graphviz_label(self) -> str:
        return self.edge_name

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return self.edge_name, self._tail_node, self._head_node

    @classmethod
    def get_instance(cls, edge_name: str, tail_node: FlowNode, head_node: FlowNode) -> FlowEdge:  # noqa: D102
        return cls._get_singleton_by_kwargs(
            edge_name=edge_name,
            _tail_node=tail_node,
            _head_node=head_node,
        )


@dataclass
class FlowGraph(MutableGraph[FlowNode, FlowEdge], MetricFlowPrettyFormattable):  # noqa: D101
    flow_name: str
    # _nodes: OrderedSet[FlowNode]
    # _edges: OrderedSet[FlowEdge]

    # @staticmethod
    # def create(flow_name: str, nodes: OrderedSet[FlowNode], edges: OrderedSet[FlowEdge]) -> FlowGraph:  # noqa: D102
    #     return FlowGraph(
    #         flow_name=flow_name,
    #         _nodes=nodes,
    #         _edges=edges,
    #     )
    #
    # @override
    # def nodes(self) -> OrderedSet[FlowNode]:
    #     return self._nodes
    #
    # @override
    # def edges(self) -> OrderedSet[FlowEdge]:
    #     return self._edges

    @staticmethod
    def create(flow_name: str, nodes: Iterable[FlowNode] = (), edges: Iterable[FlowEdge] = ()) -> FlowGraph:
        graph = FlowGraph(
            _nodes=MutableOrderedSet(),
            _edges=MutableOrderedSet(),
            _node_property_to_nodes=defaultdict(MutableOrderedSet),
            _tail_node_to_edges=defaultdict(MutableOrderedSet),
            _head_node_to_edges=defaultdict(MutableOrderedSet),
            flow_name=flow_name,
        )
        for node in nodes:
            graph.add_node(node)
        for edge in edges:
            graph.add_edge(edge)
        return graph

    @property
    @override
    def dot_label(self) -> str:
        return self.flow_name

    @property
    @override
    def graphviz_label(self) -> str:
        return self.flow_name

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        formatter = format_context.formatter
        return formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping={"nodes": self.nodes, "edges": self.edges},
        )


@pytest.fixture(scope="session")
def flow_graph() -> FlowGraph:
    """Example instance of a graph used in test cases."""
    source_node = FlowNode.get_instance("source")
    a_node = FlowNode.get_instance("a")
    b_node = FlowNode.get_instance("b")
    sink_node = FlowNode.get_instance("sink")

    return FlowGraph.create(
        flow_name="example_flow",
        nodes=OrderedSet.create_from_items(source_node, a_node, b_node, sink_node),
        edges=OrderedSet.create_from_items(
            FlowEdge.get_instance("source_to_a", tail_node=source_node, head_node=a_node),
            FlowEdge.get_instance("source_to_b", tail_node=source_node, head_node=b_node),
            FlowEdge.get_instance("a_to_sink", tail_node=a_node, head_node=sink_node),
            FlowEdge.get_instance("b_to_sink", tail_node=b_node, head_node=sink_node),
        ),
    )


def test_pretty_format_graph(
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, flow_graph: FlowGraph
) -> None:
    """Check formatting of the graph using `pretty_format`."""
    source_node = FlowNode.get_instance("source")
    a_node = FlowNode.get_instance("a")
    b_node = FlowNode.get_instance("b")
    sink_node = FlowNode.get_instance("sink")

    graph = FlowGraph.create(
        flow_name="example_flow",
        nodes=OrderedSet.create_from_items(source_node, a_node, b_node, sink_node),
        edges=OrderedSet.create_from_items(
            FlowEdge.get_instance("source_to_a", tail_node=source_node, head_node=a_node),
            FlowEdge.get_instance("source_to_b", tail_node=source_node, head_node=b_node),
            FlowEdge.get_instance("a_to_sink", tail_node=a_node, head_node=sink_node),
            FlowEdge.get_instance("b_to_sink", tail_node=b_node, head_node=sink_node),
        ),
    )

    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj=graph,
    )


def test_dot_notation(
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, flow_graph: FlowGraph
) -> None:
    """Check formatting of the graph using `dot_notation`."""
    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_str=flow_graph.format_dot(),
        snapshot_id="result",
    )


def test_graph_snapshot(
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, flow_graph: FlowGraph
) -> None:
    """Check the graph snapshot."""
    assert_graph_snapshot_equal(request=request, snapshot_configuration=mf_test_configuration, graph=flow_graph)
