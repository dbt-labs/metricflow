from __future__ import annotations

import pytest
from metricflow_semantics.experimental.ordered_set import MutableOrderedSet

from tests_metricflow_semantics.experimental.mf_graph.flow_graph import (
    FlowEdge,
    FlowGraph,
    IntermediateNode,
    SinkNode,
    SourceNode,
)


@pytest.fixture(scope="module")
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
                FlowEdge.get_instance(tail_node=source_node, head_node=a_node, weight=1),
                FlowEdge.get_instance(tail_node=source_node, head_node=b_node, weight=2),
                FlowEdge.get_instance(tail_node=a_node, head_node=b_node, weight=0),
                FlowEdge.get_instance(tail_node=a_node, head_node=sink_node, weight=2),
                FlowEdge.get_instance(tail_node=b_node, head_node=sink_node, weight=1),
            )
        ),
    )
