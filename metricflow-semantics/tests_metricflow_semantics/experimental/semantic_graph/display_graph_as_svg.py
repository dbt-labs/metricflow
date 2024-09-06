from __future__ import annotations

import logging
import os

import graphviz
from metricflow_semantics.experimental.semantic_graph.semantic_graph import SemanticGraph
from metricflow_semantics.random_id import random_id

logger = logging.getLogger(__name__)


def display_graph_as_svg(semantic_graph: SemanticGraph, directory_path: str) -> str:
    """Create and display the plan as an SVG in the browser.

    Returns the path where the SVG file was created within "mf_config_dir".
    """
    svg_dir = os.path.join(directory_path, "generated_svg")
    random_file_path = os.path.join(svg_dir, f"graph_{random_id()}")
    render_via_graphviz(semantic_graph=semantic_graph, file_path_without_svg_suffix=random_file_path)
    return random_file_path + ".svg"


def render_via_graphviz(semantic_graph: SemanticGraph, file_path_without_svg_suffix: str) -> None:
    """Render the DAG using graphviz.

    Args:
        semantic_graph: The graph to render.
        file_path_without_svg_suffix: Path to the SVG file that should be created, without ".svg" suffix.
    """
    dot = graphviz.Digraph(comment="Semantic Graph", node_attr={"shape": "box", "fontname": "Courier"})
    for node in semantic_graph.nodes:
        dot.node(name=node.dot_label, label=node.graphviz_label)

    for edge in semantic_graph.edges:
        dot.edge(tail_name=edge.tail_node.dot_label, head_name=edge.head_node.dot_label)

    dot.format = "svg"
    dot.render(file_path_without_svg_suffix, view=True, format="svg", cleanup=True)
