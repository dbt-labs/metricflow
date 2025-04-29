from __future__ import annotations

import logging
from pathlib import Path
from typing import TypeVar

import graphviz
from metricflow_semantics.dag.mf_dag import DagNode
from metricflow_semantics.experimental.mf_graph.formatting.graph_formatter import GraphFormatter
from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraph
from metricflow_semantics.helpers.string_helpers import mf_dedent
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

logger = logging.getLogger(__name__)
DagNodeT = TypeVar("DagNodeT", bound=DagNode)

#
# def add_nodes_to_digraph(node: DagNodeT, dot: graphviz.Digraph) -> None:
#     """Adds the node (and parent nodes) to the dot for visualization."""
#     for parent_node in node.parent_nodes:
#         add_nodes_to_digraph(parent_node, dot)
#
#     dot.node(name=node.node_id.id_str, label=node.graphviz_label)
#     for parent_node in node.parent_nodes:
#         dot.edge(tail_name=parent_node.node_id.id_str, head_name=node.node_id.id_str)
#
#
# def display_graph_as_svg(semantic_graph_old: MetricflowGraph[], directory_path: str) -> str:
#     """Create and display the plan as an SVG in the browser.
#
#     Returns the path where the SVG file was created within "mf_config_dir".
#     """
#     svg_dir = os.path.join(directory_path, "generated_svg")
#     random_file_path = os.path.join(svg_dir, f"dag_{random_id()}")
#     render_via_graphviz(semantic_graph_old=semantic_graph_old, file_path_without_svg_suffix=random_file_path)
#     return random_file_path + ".svg"
#
#
# def render_via_graphviz(semantic_graph_old: MetricflowGraph, file_path_without_svg_suffix: str) -> None:
#     """Render the DAG using graphviz."""
#     dot = graphviz.Digraph(
#         comment=semantic_graph_old.graph_id.str_value,
#         graph_attr={
#             "splines": "true",
#             # "concentrate": "true",
#         },
#         node_attr={
#             "shape": "box",
#             "fontname": "Courier New",
#         },
#         edge_attr={"fontname": "Courier New", "fontsize": "10"},
#         format="svg",
#     )
#     # Not quite correct if there are shared nodes.
#     for node in semantic_graph_old.nodes:
#         dot.node(name=node.dot_label, label=node.graphviz_label)
#     for edge in semantic_graph_old.edges:
#         dot.edge(
#             tail_name=edge.tail_node.dot_label,
#             head_name=edge.head_node.dot_label,
#             label=edge.graphviz_label,
#         )
#     logger.info(f"Writing to {repr(file_path_without_svg_suffix + '.svg')}")
#     dot.render(file_path_without_svg_suffix, view=True, format="svg", cleanup=True)
#
#
# def display_graph_if_requested(
#     mf_test_configuration: MetricFlowTestConfiguration, request: FixtureRequest, semantic_graph_old: SemanticGraph
# ) -> None:
#     """Create and display the plan as an SVG, if requested to do so."""
#     if not mf_test_configuration.display_graphs:
#         return
#
#     if len(request.session.items) > 1:
#         raise ValueError("Displaying graphs is only supported when there's a single item in a testing session.")
#
#     plan_svg_output_path_prefix = str(
#         snapshot_path_prefix(
#             request=request,
#             snapshot_configuration=mf_test_configuration,
#             snapshot_group=semantic_graph_old.__class__.__name__,
#             snapshot_id=semantic_graph_old.graph_id.str_value,
#         )
#     )
#
#     # Create parent directory since it might not exist
#     os.makedirs(os.path.dirname(plan_svg_output_path_prefix), exist_ok=True)
#     render_via_graphviz(semantic_graph_old=semantic_graph_old, file_path_without_svg_suffix=plan_svg_output_path_prefix)


class SvgFileFormatter(GraphFormatter[None]):
    _EXPECTED_OUTPUT_FILE_SUFFIX = ".svg"

    def __init__(self, output_svg_file_path: Path) -> None:
        self._output_svg_file_path = output_svg_file_path
        if output_svg_file_path.suffix != SvgFileFormatter._EXPECTED_OUTPUT_FILE_SUFFIX:
            raise ValueError(
                LazyFormat(
                    "The provided path does not match the expected suffix",
                    output_svg_file_path=output_svg_file_path,
                    expected_suffix=SvgFileFormatter._EXPECTED_OUTPUT_FILE_SUFFIX,
                )
            )
        self._output_file_path_without_suffix = str(self._output_svg_file_path)[
            : -len(self._EXPECTED_OUTPUT_FILE_SUFFIX)
        ]

    @override
    def format_graph(self, graph: MetricflowGraph) -> None:
        """Render the DAG using graphviz."""
        dot = graphviz.Digraph(
            comment=graph.graphviz_label,
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
        for node in graph.nodes:
            dot.node(name=node.dot_label, label=node.graphviz_label)
        for edge in graph.edges:
            dot.edge(
                tail_name=edge.tail_node.graphviz_label,
                head_name=edge.head_node.graphviz_label,
                # Put the label in a table so that `graphviz` puts the edges a little bit away from the nodes.
                label=mf_dedent(
                    f"""
                    <<table cellpadding="10" border="0" cellborder="0">
                        <tr><td>{edge.graphviz_label}</td></tr>
                      </table>>
                    """
                ),
            )

        logger.debug(LazyFormat("Writing SVG file of the graph to a file", svg_file=self._output_svg_file_path))
        with open(self._output_svg_file_path, "w") as output_file:
            output_file.write(dot.pipe(format="svg").decode("utf-8"))
        logger.info(
            LazyFormat("Wrote SVG file of the graph", dot_label=graph.dot_label, svg_file=self._output_svg_file_path)
        )
