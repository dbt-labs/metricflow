from __future__ import annotations

import pytest
from metricflow_semantics.toolkit.collections.ordered_set import MutableOrderedSet

from tests_metricflow_semantics.toolkit.mf_graph.flow_graph import (
    FlowEdge,
    FlowGraph,
    IntermediateNode,
    SinkNode,
    SourceNode,
)


@pytest.fixture(scope="module")
def flow_graph() -> FlowGraph:
    """Example instance of a graph used in test cases."""
    source_node = SourceNode.get_instance(node_name="source")
    a_node = IntermediateNode.get_instance(node_name="a")
    b_node = IntermediateNode.get_instance(node_name="b")
    sink_node = SinkNode.get_instance(node_name="sink")

    return FlowGraph.create(
        nodes=MutableOrderedSet((source_node, a_node, b_node, sink_node)),
        edges=MutableOrderedSet(
            (
                FlowEdge(tail_node=source_node, head_node=a_node, weight=1),
                FlowEdge(tail_node=source_node, head_node=b_node, weight=2),
                FlowEdge(tail_node=a_node, head_node=b_node, weight=0),
                FlowEdge(tail_node=a_node, head_node=sink_node, weight=2),
                FlowEdge(tail_node=b_node, head_node=sink_node, weight=1),
            )
        ),
    )
