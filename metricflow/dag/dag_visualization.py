from __future__ import annotations

import logging
import os
from typing import TypeVar

import graphviz

from metricflow.dag.mf_dag import DagNode, MetricFlowDag
from metricflow.random_id import random_id

logger = logging.getLogger(__name__)
DagNodeT = TypeVar("DagNodeT", bound=DagNode)


def add_nodes_to_digraph(node: DagNodeT, dot: graphviz.Digraph) -> None:
    """Adds the node (and parent nodes) to the dot for visualization."""
    for parent_node in node.parent_nodes:
        add_nodes_to_digraph(parent_node, dot)

    dot.node(name=node.node_id.id_str, label=node.graphviz_label)
    for parent_node in node.parent_nodes:
        dot.edge(tail_name=parent_node.node_id.id_str, head_name=node.node_id.id_str)


DagGraphT = TypeVar("DagGraphT", bound=MetricFlowDag)


def display_dag_as_svg(dag_graph: DagGraphT, directory_path: str) -> str:
    """Create and display the plan as an SVG in the browser.

    Returns the path where the SVG file was created within "mf_config_dir".
    """
    svg_dir = os.path.join(directory_path, "generated_svg")
    random_file_path = os.path.join(svg_dir, f"dag_{random_id()}")
    render_via_graphviz(dag_graph=dag_graph, file_path_without_svg_suffix=random_file_path)
    return random_file_path + ".svg"


def render_via_graphviz(dag_graph: DagGraphT, file_path_without_svg_suffix: str) -> None:
    """Render the DAG using graphviz.

    Args:
        dag_graph: The DAG to render.
        file_path_without_svg_suffix: Path to the SVG file that should be created, without ".svg" suffix.
    """
    dot = graphviz.Digraph(comment=dag_graph.dag_id, node_attr={"shape": "box", "fontname": "Courier"})
    # Not quite correct if there are shared nodes.
    for sink_node in dag_graph.sink_nodes:
        add_nodes_to_digraph(sink_node, dot)
    dot.format = "svg"
    dot.render(file_path_without_svg_suffix, view=True, format="svg", cleanup=True)
