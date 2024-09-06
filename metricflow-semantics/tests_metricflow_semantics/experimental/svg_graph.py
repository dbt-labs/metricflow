from __future__ import annotations

import logging
import os
from typing import TypeVar

import graphviz
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.dag.mf_dag import DagNode
from metricflow_semantics.experimental.semantic_graph.semantic_graph import SemanticGraph
from metricflow_semantics.random_id import random_id
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import snapshot_path_prefix

logger = logging.getLogger(__name__)
DagNodeT = TypeVar("DagNodeT", bound=DagNode)


def add_nodes_to_digraph(node: DagNodeT, dot: graphviz.Digraph) -> None:
    """Adds the node (and parent nodes) to the dot for visualization."""
    for parent_node in node.parent_nodes:
        add_nodes_to_digraph(parent_node, dot)

    dot.node(name=node.node_id.id_str, label=node.graphviz_label)
    for parent_node in node.parent_nodes:
        dot.edge(tail_name=parent_node.node_id.id_str, head_name=node.node_id.id_str)


def display_graph_as_svg(semantic_graph: SemanticGraph, directory_path: str) -> str:
    """Create and display the plan as an SVG in the browser.

    Returns the path where the SVG file was created within "mf_config_dir".
    """
    svg_dir = os.path.join(directory_path, "generated_svg")
    random_file_path = os.path.join(svg_dir, f"dag_{random_id()}")
    render_via_graphviz(semantic_graph=semantic_graph, file_path_without_svg_suffix=random_file_path)
    return random_file_path + ".svg"


def render_via_graphviz(semantic_graph: SemanticGraph, file_path_without_svg_suffix: str) -> None:
    """Render the DAG using graphviz."""
    dot = graphviz.Digraph(
        comment=semantic_graph.graph_id.str_value,
        graph_attr={
            "splines": "true",
            # "concentrate": "true",
        },
        node_attr={
            "shape": "box",
            "fontname": "Courier New",
        },
        edge_attr={"fontname": "Courier New", "fontsize": "10"},
        format="svg",
    )
    # Not quite correct if there are shared nodes.
    for node in semantic_graph.nodes:
        dot.node(name=node.dot_label, label=node.graphviz_label)
    for edge in semantic_graph.edges:
        dot.edge(
            tail_name=edge.tail_node.dot_label,
            head_name=edge.head_node.dot_label,
            label=edge.graphviz_label,
        )
    logger.info(f"Writing to {repr(file_path_without_svg_suffix + '.svg')}")
    dot.render(file_path_without_svg_suffix, view=True, format="svg", cleanup=True)


def display_graph_if_requested(
    mf_test_configuration: MetricFlowTestConfiguration, request: FixtureRequest, semantic_graph: SemanticGraph
) -> None:
    """Create and display the plan as an SVG, if requested to do so."""
    if not mf_test_configuration.display_graphs:
        return

    if len(request.session.items) > 1:
        raise ValueError("Displaying graphs is only supported when there's a single item in a testing session.")

    plan_svg_output_path_prefix = str(
        snapshot_path_prefix(
            request=request,
            snapshot_configuration=mf_test_configuration,
            snapshot_group=semantic_graph.__class__.__name__,
            snapshot_id=semantic_graph.graph_id.str_value,
        )
    )

    # Create parent directory since it might not exist
    os.makedirs(os.path.dirname(plan_svg_output_path_prefix), exist_ok=True)
    render_via_graphviz(semantic_graph=semantic_graph, file_path_without_svg_suffix=plan_svg_output_path_prefix)
