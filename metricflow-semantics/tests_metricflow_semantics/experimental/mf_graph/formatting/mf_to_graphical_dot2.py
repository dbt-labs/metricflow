# from __future__ import annotations
#
# import contextlib
# import logging
# import textwrap
# import typing
# from collections import defaultdict
# from collections.abc import Mapping
# from dataclasses import dataclass
# from typing import Iterable, Optional
#
# import graphviz
# from graphviz import Digraph
# from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
# from metricflow_semantics.dag.mf_dag import DisplayedProperty
# from metricflow_semantics.experimental.mf_graph.formatting.graphviz_attributes import (
#     DotColor,
# )
# from metricflow_semantics.experimental.mf_graph.formatting.graphviz_html import (
#     GraphvizHtmlAlignment,
#     GraphvizHtmlText,
#     GraphvizHtmlTextStyle,
# )
# from metricflow_semantics.experimental.mf_graph.formatting.graphviz_html_table_builder import GraphvizHtmlTableBuilder
# from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraphEdge, MetricflowGraphNode
# from metricflow_semantics.helpers.string_helpers import mf_indent
# from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
#
# from tests_metricflow_semantics.experimental.mf_graph.formatting.mf_to_dot import MetricflowGraphToDotConverter
#
# if typing.TYPE_CHECKING:
#     from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraph
#
# logger = logging.getLogger(__name__)
#
#
# @dataclass
# class DotGraphConversionResult:
#     """The results of converting to a DOT graph.
#
#     Since this is just used to pass data between calls, using a regular `dataclass` so that it can directly store a
#     dict.
#     """
#
#     dot_graph: Digraph
#     # For debugging context.
#     node_kwarg_dicts: AnyLengthTuple[Mapping[str, str]]
#     edge_kwarg_dicts: AnyLengthTuple[Mapping[str, str]]
#
#
# class MetricflowGraphToGraphicalDotGraphConverter(MetricflowGraphToDotConverter):
#     # Default attributes for elements in the DOT graph.
#     DEFAULT_GRAPH_ATTRIBUTES: Mapping[str, str] = {
#         "splines": "false",
#         "bgcolor": DotColor.DARK_GRAY.value,
#         # "nodesep": "3",
#         # "ranksep": "3",
#         "rankdir": "LR",
#         # "layout": "fdp",
#         # "sep": "2",
#     }
#
#     DEFAULT_NODE_ATTRIBUTES: Mapping[str, str] = {
#         "fontcolor": DotColor.LIGHT_GRAY.value,
#         "style": "filled",
#         "color": DotColor.LIGHT_GRAY.value,
#         "fillcolor": DotColor.EXTRA_DIM_GRAY.value,
#         "fontname": "Courier New",
#     }
#
#     DEFAULT_EDGE_ATTRIBUTES: Mapping[str, str] = {
#         "fontname": "Courier New",
#         "fontsize": "10",
#         "color": DotColor.LIGHT_GRAY.value,
#         "fontcolor": DotColor.LIGHT_GRAY.value,
#     }
#
#     DEFAULT_CLUSTER_ATTRIBUTES: Mapping[str, str] = {
#         "fontname": "Courier New",
#         "fontsize": "10",
#         "fontcolor": DotColor.LIGHT_GRAY.value,
#         "color": DotColor.LIGHT_GRAY.value,
#         "pencolor": DotColor.BLACK.value,
#         "penwidth": "3",
#     }
#
#     DEFAULT_EDGE_AS_NODE_ATTRIBUTES: Mapping[str, str] = {
#         "fontcolor": DotColor.LIGHT_GRAY.value,
#         "style": "filled",
#         "color": DotColor.TRANSPARENT.value,
#         "fillcolor": DotColor.TRANSPARENT.value,
#         "fontname": "Courier New",
#         "shape": "box",
#         "fontsize": "10",
#         "margin": "0.0,0.0",
#         "width": "0.0",
#         "height": "0.0",
#     }
#
#     _NODE_NAME_KEY = "name"
#     _HEAD_NAME_KEY = "head_name"
#     _TAIL_NAME_KEY = "tail_name"
#     _ARROWHEAD_KEY = "arrowhead"
#
#     def __init__(
#         self,
#         replace_edges_with_intermediate_node: bool = True,
#         default_graph_attributes: Mapping[str, str] = DEFAULT_GRAPH_ATTRIBUTES,
#         default_cluster_attributes: Mapping[str, str] = DEFAULT_CLUSTER_ATTRIBUTES,
#         default_node_attributes: Mapping[str, str] = DEFAULT_NODE_ATTRIBUTES,
#         default_edge_as_node_attributes: Mapping[str, str] = DEFAULT_EDGE_AS_NODE_ATTRIBUTES,
#         default_edge_attributes: Mapping[str, str] = DEFAULT_EDGE_ATTRIBUTES,
#     ) -> None:
#         super.__init__()
#         self._include_only_required_dot_attributes = include_only_required_dot_attributes
#         self._default_graph_attributes = default_graph_attributes
#         self._default_cluster_attributes = default_cluster_attributes
#         self._default_node_attributes = default_node_attributes
#         self._replace_edges_with_intermediate_node = replace_edges_with_intermediate_node
#         self._default_edge_as_node_attributes = dict(default_edge_as_node_attributes)
#         self._default_edge_attributes = default_edge_attributes
#
#     def convert_graph(self, graph: MetricflowGraph) -> DotGraphConversionResult:
#         dot = graphviz.Digraph(
#             name=graph.__class__.__name__,
#             graph_attr=self._default_graph_attributes if not self._include_only_required_dot_attributes else {},
#             node_attr=self._default_node_attributes if not self._include_only_required_dot_attributes else {},
#             edge_attr=self._default_node_attributes if not self._include_only_required_dot_attributes else {},
#         )
#         # Group nodes by the cluster name for visualization.
#         cluster_name_to_nodes: dict[Optional[str], list[MetricflowGraphNode]] = defaultdict(list)
#         # Record the args use to create the nodes and edges for debug context.
#         node_kwarg_dicts = []
#         edge_kwarg_dicts = []
#
#         edge_to_edge_as_node_mapping: dict[MetricflowGraphEdge, dict[str, str]] = {}
#         edge_as_node_index = 0
#         if self._replace_edges_with_intermediate_node:
#             for edge in graph.edges:
#                 edge_displayed_properties = edge.displayed_properties
#                 if len(edge_displayed_properties) > 0:
#                     edge_to_edge_as_node_mapping[edge] = self._default_edge_as_node_attributes | {
#                         self._NODE_NAME_KEY: f"Edge Intermediate Node #{edge_as_node_index}",
#                         "label": self._make_edge_label(edge_displayed_properties),
#                     }
#                     edge_as_node_index += 1
#
#         # If the node has a `cluster_name`, add the nodes in a DOT subgraph so that they can be
#         # displayed as a group.
#         for node in sorted(graph.nodes):
#             cluster_name_to_nodes[node.node_descriptor.cluster_name].append(node)
#
#         for cluster_name, nodes in cluster_name_to_nodes.items():
#             if cluster_name is None:
#                 continue
#             subgraph_context = (
#                 # Graphviz needs the subgraph to be prefixed with the constant.
#                 dot.subgraph(name="cluster_" + cluster_name)
#                 if cluster_name is not None
#                 else contextlib.nullcontext(enter_result=dot)
#             )
#
#             with subgraph_context as subgraph:
#                 if cluster_name is not None and not self._include_only_required_dot_attributes:
#                     subgraph.attr(label=cluster_name, **MetricflowGraphToDotConverter.DEFAULT_CLUSTER_ATTRIBUTERS)
#
#                 for node in nodes:
#                     node_kwarg_dict = node.dot_attributes.as_kwargs()
#                     node_kwarg_dict.update({"label": self._make_node_label(node)})
#                     if self._include_only_required_dot_attributes:
#                         required_keys = {self._NODE_NAME_KEY}
#                         node_kwarg_dict = {key: value for key, value in node_kwarg_dict.items() if key in required_keys}
#                     node_kwarg_dicts.append(node_kwarg_dict)
#                     subgraph.node(**node_kwarg_dict)
#
#         for edge, node_kwargs_dict in edge_to_edge_as_node_mapping.items():
#             dot.node(**node_kwargs_dict)
#             node_kwarg_dicts.append(node_kwarg_dict)
#
#         for edge in sorted(graph.edges):
#             edge_kwarg_dict = edge.dot_attributes.as_kwargs()
#             if self._include_only_required_dot_attributes:
#                 required_keys = {self._TAIL_NAME_KEY, self._HEAD_NAME_KEY}
#                 edge_kwarg_dict = {key: value for key, value in edge_kwarg_dict.items() if key in required_keys}
#
#             if edge in edge_to_edge_as_node_mapping:
#                 edge_as_node_kwargs = edge_to_edge_as_node_mapping[edge]
#                 tail_node_to_edge_node_kwargs = edge_kwarg_dict | {
#                     self._TAIL_NAME_KEY: edge_kwarg_dict[self._TAIL_NAME_KEY],
#                     self._HEAD_NAME_KEY: edge_as_node_kwargs[self._NODE_NAME_KEY],
#                     self._ARROWHEAD_KEY: "none",
#                 }
#                 dot.edge(**tail_node_to_edge_node_kwargs)
#                 edge_kwarg_dicts.append(tail_node_to_edge_node_kwargs)
#
#                 edge_node_to_tail_node_kwargs = edge_kwarg_dict | {
#                     self._TAIL_NAME_KEY: edge_as_node_kwargs[self._NODE_NAME_KEY],
#                     self._HEAD_NAME_KEY: edge_kwarg_dict[self._HEAD_NAME_KEY],
#                 }
#                 dot.edge(**edge_node_to_tail_node_kwargs)
#                 edge_kwarg_dicts.append(edge_node_to_tail_node_kwargs)
#             else:
#                 dot.edge(**edge_kwarg_dict)
#                 edge_kwarg_dicts.append(edge_kwarg_dict)
#
#         logger.debug(LazyFormat("Created dot graph", node_kwarg_dicts=node_kwarg_dicts))
#
#         return DotGraphConversionResult(
#             dot_graph=dot,
#             node_kwarg_dicts=tuple(node_kwarg_dicts),
#             edge_kwarg_dicts=tuple(edge_kwarg_dicts),
#         )
#
#     def _add_displayed_properties_to_label_table(
#         self,
#         table_builder: GraphvizHtmlTableBuilder,
#         displayed_properties: Iterable[DisplayedProperty],
#         word_wrap_limit: int = 30,
#     ) -> None:
#         for displayed_property in displayed_properties:
#             key = displayed_property.key
#             value_lines = textwrap.wrap(str(displayed_property.value), width=word_wrap_limit)
#             # <BR> seems to have odd formatting issues, so using multiple rows to display mult-line text.
#             if len(value_lines) == 0:
#                 continue
#
#             if len(value_lines) == 1:
#                 with table_builder.new_row_builder() as row_builder:
#                     row_builder.add_column(
#                         GraphvizHtmlText(
#                             f"{key}: {value_lines[0]}",
#                             GraphvizHtmlTextStyle.DESCRIPTION,
#                         ),
#                         alignment=GraphvizHtmlAlignment.LEFT,
#                     )
#             else:
#                 for row_index, value_line in enumerate(value_lines):
#                     with table_builder.new_row_builder() as row_builder:
#                         if row_index == 0:
#                             row_builder.add_column(
#                                 GraphvizHtmlText(f"{key}:", style=GraphvizHtmlTextStyle.DESCRIPTION),
#                                 alignment=GraphvizHtmlAlignment.LEFT,
#                             )
#                         else:
#                             row_builder.add_column(
#                                 GraphvizHtmlText(
#                                     mf_indent(value_line),
#                                     GraphvizHtmlTextStyle.DESCRIPTION,
#                                 ),
#                                 alignment=GraphvizHtmlAlignment.LEFT,
#                             )
#
#     def _make_node_label(self, node: MetricflowGraphNode) -> str:
#         """Return a `graphviz` / DOT label to render this node.
#
#         See https://graphviz.org/doc/info/shapes.html#html for HTML formatting.
#
#         Currently, this return a `graphviz` HTML table where the first row is 1 column with the ID of
#         the node, and subsequent rows have 2 columns to show the `node.displayed_properties` (key and value).
#         """
#         table_builder = GraphvizHtmlTableBuilder()
#
#         with table_builder.new_row_builder() as row_builder:
#             row_builder.add_column(
#                 GraphvizHtmlText(node.node_descriptor.node_name, style=GraphvizHtmlTextStyle.TITLE),
#                 alignment=GraphvizHtmlAlignment.CENTER,
#                 # column_span=2,
#                 cell_padding=4,
#             )
#
#         self._add_displayed_properties_to_label_table(table_builder, node.displayed_properties)
#
#         result = "\n".join(table_builder.build())
#         # logger.debug(LazyFormat("Generated HTML label for a node", node=self, html=result))
#         return result
#
#     def _make_edge_label(self, displayed_properties: Iterable[DisplayedProperty]) -> str:
#         """Similar to `_make_node_label`"""
#         table_builder = GraphvizHtmlTableBuilder(table_border=1)
#         self._add_displayed_properties_to_label_table(table_builder, displayed_properties)
#         return "\n".join(table_builder.build())
