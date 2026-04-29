from __future__ import annotations

import logging
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    import graphviz
from metricflow_semantics.dag.mf_dag import DagNode, MetricFlowDag

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


def render_via_graphviz(dag_graph: DagGraphT, file_path_without_svg_suffix: str) -> None:
    """Render the DAG using graphviz.

    Args:
        dag_graph: The DAG to render.
        file_path_without_svg_suffix: Path to the SVG file that should be created, without ".svg" suffix.

    Raises:
        RuntimeError: If the ``graphviz`` package is not installed.
    """
    # `graphviz` is an optional dependency as rendering graphs is more of a debugging feature in tests / CLI.
    # Since it's optional, import it locally only when needed.
    try:
        import graphviz
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "The `graphviz` Python package is required for DAG visualization."
            " It can be installed with `pip install graphviz` or similar."
            " Rendering may also require the `dot` executable from https://www.graphviz.org/ to be on your PATH."
        ) from error
    dot = graphviz.Digraph(comment=dag_graph.dag_id, node_attr={"shape": "box", "fontname": "Courier"})
    # Not quite correct if there are shared nodes.
    for sink_node in dag_graph.sink_nodes:
        add_nodes_to_digraph(sink_node, dot)
    dot.format = "svg"
    dot.render(file_path_without_svg_suffix, view=True, format="svg", cleanup=True)
