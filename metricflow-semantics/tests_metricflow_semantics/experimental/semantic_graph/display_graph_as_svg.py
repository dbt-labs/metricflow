from __future__ import annotations

import logging
import os

import graphviz
from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraph
from metricflow_semantics.random_id import random_id

logger = logging.getLogger(__name__)


def display_graph_as_svg(graph: MetricflowGraph, directory_path: str) -> str:
    """Create and display the plan as an SVG in the browser.

    Returns the path where the SVG file was created within "mf_config_dir".
    """
    svg_dir = os.path.join(directory_path, "generated_svg")
    random_file_path = os.path.join(svg_dir, f"graph_{random_id()}")
    render_via_graphviz(graph=graph, file_path_without_svg_suffix=random_file_path)
    return random_file_path + ".svg"


def render_via_graphviz(graph: MetricflowGraph, file_path_without_svg_suffix: str) -> None:
    """Render the DAG using graphviz.

    Args:
        graph: The graph to render.
        file_path_without_svg_suffix: Path to the SVG file that should be created, without ".svg" suffix.
    """
    dot = graphviz.Digraph(comment=graph.graphviz_label, node_attr={"shape": "box", "fontname": "Courier"})
    for node in graph.nodes:
        dot.node(name=node.dot_label, label=node.graphviz_label)

    for edge in graph.edges:
        dot.edge(tail_name=edge._tail_node.dot_label, head_name=edge._head_node.dot_label)

    dot.format = "svg"
    dot.render(file_path_without_svg_suffix, view=True, format="svg", cleanup=True)
