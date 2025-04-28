from __future__ import annotations

import logging
from abc import ABC
from functools import cached_property
from typing import Optional

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.collection_helpers.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
from metricflow_semantics.experimental.mf_graph.formatting.dot_formatter import DotNotationFormatter
from metricflow_semantics.experimental.mf_graph.mf_graph import (
    MetricflowGraph,
    MetricflowGraphEdge,
    MetricflowGraphNode,
)
from metricflow_semantics.experimental.singleton import Singleton
from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.mf_logging.pretty_formatter import PrettyFormatContext
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_object_snapshot_equal, assert_str_snapshot_equal
from typing_extensions import override

from tests_metricflow_semantics.experimental.graph_helpers import assert_graph_snapshot_equal

logger = logging.getLogger(__name__)


@fast_frozen_dataclass(order=False)
class FlowNode(MetricflowGraphNode, Singleton, ABC):
    node_name: str

    @cached_property
    def dot_label(self) -> str:
        return self.node_name

    @cached_property
    def graphviz_label(self) -> str:
        return self.node_name

    @cached_property
    def comparison_key(self) -> ComparisonKey:
        return (self.node_name,)

    @classmethod
    def get_instance(cls, node_name: str) -> FlowNode:
        return cls._get_singleton_by_kwargs(node_name=node_name)


@fast_frozen_dataclass(order=False)
class SourceNode(FlowNode):
    pass


@fast_frozen_dataclass(order=False)
class SinkNode(FlowNode):
    pass


@fast_frozen_dataclass(order=False)
class IntermediateNode(FlowNode):
    pass


@fast_frozen_dataclass(order=False)
class FlowEdge(MetricflowGraphEdge, Singleton):
    edge_name: str
    _tail_node: FlowNode
    _head_node: FlowNode

    @property
    def tail_node(self) -> FlowNode:
        return self._tail_node

    @property
    def head_node(self) -> FlowNode:
        return self._head_node

    @cached_property
    def inverse(self) -> FlowEdge:
        return FlowEdge.get_instance(edge_name=self.edge_name, tail_node=self._head_node, head_node=self._tail_node)

    @cached_property
    def dot_label(self) -> str:
        return self.edge_name

    @cached_property
    def graphviz_label(self) -> str:
        return self.edge_name

    @cached_property
    def comparison_key(self) -> ComparisonKey:
        return self.edge_name, self._tail_node, self._head_node

    @classmethod
    def get_instance(cls, edge_name: str, tail_node: FlowNode, head_node: FlowNode) -> FlowEdge:
        return cls._get_singleton_by_kwargs(
            edge_name=edge_name,
            _tail_node=tail_node,
            _head_node=head_node,
        )


@fast_frozen_dataclass(order=False)
class FlowGraph(MetricflowGraph[FlowNode, FlowEdge], MetricFlowPrettyFormattable):
    flow_name: str
    _nodes: FrozenOrderedSet[FlowNode]
    _edges: FrozenOrderedSet[FlowEdge]

    @staticmethod
    def create(flow_name: str, nodes: FrozenOrderedSet[FlowNode], edges: FrozenOrderedSet[FlowEdge]) -> FlowGraph:
        return FlowGraph(
            flow_name=flow_name,
            _nodes=nodes,
            _edges=edges,
        )

    @property
    def nodes(self) -> FrozenOrderedSet[FlowNode]:
        return self._nodes

    @property
    def edges(self) -> FrozenOrderedSet[FlowEdge]:
        return self._edges

    @cached_property
    def inverse(self) -> FlowGraph:
        return FlowGraph.create(
            flow_name=self.flow_name,
            nodes=self._nodes,
            edges=FrozenOrderedSet.create_from_iterable(edge.inverse for edge in self._edges),
        )

    @cached_property
    def dot_label(self) -> str:
        return self.flow_name

    @cached_property
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
    source_node = FlowNode.get_instance("source")
    a_node = FlowNode.get_instance("a")
    b_node = FlowNode.get_instance("b")
    sink_node = FlowNode.get_instance("sink")

    return FlowGraph.create(
        flow_name="example_flow",
        nodes=FrozenOrderedSet.create_from_args(source_node, a_node, b_node, sink_node),
        edges=FrozenOrderedSet.create_from_args(
            FlowEdge.get_instance("source_to_a", tail_node=source_node, head_node=a_node),
            FlowEdge.get_instance("source_to_b", tail_node=source_node, head_node=b_node),
            FlowEdge.get_instance("a_to_sink", tail_node=a_node, head_node=sink_node),
            FlowEdge.get_instance("b_to_sink", tail_node=b_node, head_node=sink_node),
        ),
    )


def test_pretty_format_graph(
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, flow_graph: FlowGraph
) -> None:
    source_node = FlowNode.get_instance("source")
    a_node = FlowNode.get_instance("a")
    b_node = FlowNode.get_instance("b")
    sink_node = FlowNode.get_instance("sink")

    graph = FlowGraph.create(
        flow_name="example_flow",
        nodes=FrozenOrderedSet.create_from_args(source_node, a_node, b_node, sink_node),
        edges=FrozenOrderedSet.create_from_args(
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
    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_str=flow_graph.format_graph(DotNotationFormatter()),
        snapshot_id="result",
    )


def test_graph_snapshot(
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, flow_graph: FlowGraph
) -> None:

    assert_graph_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        graph=flow_graph
    )