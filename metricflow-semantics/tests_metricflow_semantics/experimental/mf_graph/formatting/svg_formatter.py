from __future__ import annotations

import logging
import typing

import graphviz
from metricflow_semantics.experimental.mf_graph.formatting.graph_formatter import GraphFormatter
from metricflow_semantics.helpers.string_helpers import mf_dedent
from typing_extensions import override

if typing.TYPE_CHECKING:
    from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraph

logger = logging.getLogger(__name__)


class SvgFormatter(GraphFormatter):
    """Format a graph as an SVG that can be displayed in a browser."""

    @override
    def format_graph(self, graph: MetricflowGraph) -> str:
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
        # The `dot_label` must be unique among the nodes for this to work.
        for node in graph.nodes():
            dot.node(name=node.dot_label, label=node.graphviz_label)
        for edge in graph.edges():
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

        return dot.pipe(format="svg").decode("utf-8")
