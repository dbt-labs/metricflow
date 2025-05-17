from __future__ import annotations

import logging
import typing

from metricflow_semantics.experimental.metricflow_exception import MetricflowException
from metricflow_semantics.experimental.mf_graph.formatting.graph_formatter import MetricflowGraphFormatter
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

from tests_metricflow_semantics.experimental.mf_graph.formatting.mf_to_dot import (
    MetricflowGraphToDotGraphConverter,
)

if typing.TYPE_CHECKING:
    from metricflow_semantics.experimental.mf_graph.mf_graph import (
        MetricflowGraph,
    )

logger = logging.getLogger(__name__)

#
# class SvgFormatter(MetricflowGraphFormatter):
#     """Format a graph as an SVG that can be displayed in a browser."""
#
#     @override
#     def format_graph(self, graph: MetricflowGraph) -> str:
#         """Format the graph to the SVG image format using graphviz."""
#         dot = graphviz.Digraph(
#             comment=graph.dot_attributes.name,
#             # graph_attr={
#             #     "splines": "true",
#             #     "bgcolor": "#181818",
#             # },
#             # node_attr={
#             #     "fontcolor": "#e6e6e6",
#             #     "style": "filled",
#             #     "color": "#e6e6e6",
#             #     "fillcolor": "#333333",
#             #     "fontname": "Courier New",
#             # },
#             # edge_attr={
#             #     "fontname": "Courier New",
#             #     "fontsize": "10",
#             #     "color": "#e6e6e6",
#             #     "fontcolor": "#e6e6e6",
#             # },
#             graph_attr={
#                 "splines": "true",
#                 "bgcolor": GraphvizColor.DARK_GRAY.value,
#             },
#             node_attr={
#                 "fontcolor": GraphvizColor.LIGHT_GRAY.value,
#                 "style": "filled",
#                 "color": GraphvizColor.LIGHT_GRAY.value,
#                 "fillcolor": GraphvizColor.EXTRA_DIM_GRAY.value,
#                 "fontname": "Courier New",
#             },
#             edge_attr={
#                 "fontname": "Courier New",
#                 "fontsize": "10",
#                 "color": GraphvizColor.LIGHT_GRAY.value,
#                 "fontcolor": GraphvizColor.LIGHT_GRAY.value,
#             },
#             format="svg",
#         )
#         # The `dot_label` must be unique among the nodes for this to work.
#         node_kwarg_sets = []
#         for node in sorted(graph.nodes):
#             node_graphviz_attributes = node.dot_attributes
#             node_kwarg_set = node_graphviz_attributes.as_kwargs()
#             if node_graphviz_attributes.shape is not None:
#                 node_kwarg_set["shape"] = node_graphviz_attributes.shape.value
#             node_kwarg_sets.append(node_kwarg_set)
#             dot.node(**node_kwarg_set)
#
#         edge_kwarg_sets = []
#         for edge in sorted(graph.edges):
#             edge_kwarg_set = edge.dot_attributes.as_kwargs()
#
#             tail_node_graphviz_element = edge.tail_node.dot_attributes
#             head_node_graphviz_element: DotNodeAttributeSet = edge.head_node.dot_attributes
#
#             edge_kwarg_set.update(
#                 {
#                     "tail_name": tail_node_graphviz_element.name,
#                     "head_name": head_node_graphviz_element.name,
#                     # Put the name in a table so that `graphviz` puts the edges a little bit away from the nodes.
#                     # "label": mf_dedent(
#                     #     f"""
#                     #     <<table cellpadding="10" border="0" cellborder="0">
#                     #         <tr><td>{edge.graphviz_attributes.label}</td></tr>
#                     #       </table>>
#                     #     """
#                     # ),
#                 }
#             )
#
#             dot.edge(**edge_kwarg_set)
#             edge_kwarg_sets.append(edge_kwarg_set)
#
#         try:
#             return dot.pipe(format="svg").decode("utf-8")
#         except Exception as e:
#             raise MetricflowException(
#                 LazyFormat("Error generating SVG", node_kwarg_sets=node_kwarg_sets, edge_kwarg_sets=edge_kwarg_sets)
#             ) from e


class SvgFormatter(MetricflowGraphFormatter):
    """Format a graph as an SVG that can be displayed in a browser."""

    def __init__(self) -> None:
        self._mf_to_dot_graph_converter = MetricflowGraphToDotGraphConverter(include_only_required_dot_attributes=False)

    @override
    def format_graph(self, graph: MetricflowGraph) -> str:
        """Format the graph to the SVG image format using graphviz."""
        result = self._mf_to_dot_graph_converter.convert_graph(graph)
        dot_graph = result.dot_graph
        try:
            return dot_graph.pipe(format="svg").decode("utf-8")
        except Exception as e:
            raise MetricflowException(
                LazyFormat(
                    "Error generating SVG",
                    node_kwarg_dicts=result.node_kwarg_dicts,
                    edge_kwarg_dicts=result.edge_kwarg_dicts,
                )
            ) from e
