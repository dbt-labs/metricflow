from __future__ import annotations

import logging
from abc import ABC
from functools import cached_property
from typing import Optional

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
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

    @cached_property
    @override
    def inverse(self) -> FlowEdge:
        return FlowEdge.get_instance(edge_name=self.edge_name, tail_node=self._head_node, head_node=self._tail_node)

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


@fast_frozen_dataclass(order=False)
class FlowGraph(MetricflowGraph[FlowNode, FlowEdge], MetricFlowPrettyFormattable):  # noqa: D101
    flow_name: str
    _nodes: FrozenOrderedSet[FlowNode]
    _edges: FrozenOrderedSet[FlowEdge]

    @staticmethod
    def create(  # noqa: D102
        flow_name: str, nodes: FrozenOrderedSet[FlowNode], edges: FrozenOrderedSet[FlowEdge]
    ) -> FlowGraph:
        return FlowGraph(
            flow_name=flow_name,
            _nodes=nodes,
            _edges=edges,
        )

    @override
    def nodes(self) -> FrozenOrderedSet[FlowNode]:
        return self._nodes

    @override
    def edges(self) -> FrozenOrderedSet[FlowEdge]:
        return self._edges

    @cached_property
    @override
    def inverse(self) -> FlowGraph:
        return FlowGraph.create(
            flow_name=self.flow_name,
            nodes=self._nodes,
            edges=FrozenOrderedSet.create_from_iterable(edge.inverse for edge in self._edges),
        )

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
    """Check formatting of the graph using `pretty_format`."""
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
