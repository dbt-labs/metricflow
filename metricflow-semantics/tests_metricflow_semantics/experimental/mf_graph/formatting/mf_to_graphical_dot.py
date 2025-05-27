from __future__ import annotations

import logging
import textwrap
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import ClassVar, DefaultDict, Iterable, Optional

from metricflow_semantics.collection_helpers.syntactic_sugar import (
    mf_ensure_mapping,
    mf_first_non_none,
    mf_group_by,
)
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.experimental.mf_graph.formatting.dot_attributes import (
    DotColor,
    DotEdgeArrowShape,
    DotEdgeAttributeSet,
    DotNodeAttributeSet,
    DotNodeShape,
)
from metricflow_semantics.experimental.mf_graph.graph_converter import MetricflowGraphConverter
from metricflow_semantics.experimental.mf_graph.mf_graph import (
    MetricflowGraph,
    MetricflowGraphEdge,
    MetricflowGraphNode,
)
from metricflow_semantics.helpers.string_helpers import mf_indent
from typing_extensions import override

from tests_metricflow_semantics.experimental.mf_graph.formatting.graphviz_html import (
    GraphvizHtmlAlignment,
    GraphvizHtmlText,
    GraphvizHtmlTextStyle,
)
from tests_metricflow_semantics.experimental.mf_graph.formatting.graphviz_html_table_builder import (
    GraphvizHtmlTableBuilder,
)
from tests_metricflow_semantics.experimental.mf_graph.formatting.mf_to_dot import (
    DotConversionArgumentSet,
    DotGraphConversionResult,
    DotGraphElementSet,
    MetricflowGraphToDotConverter,
)

logger = logging.getLogger(__name__)


@dataclass
class GraphicalDotConversionArgumentSet(DotConversionArgumentSet):
    default_edge_as_node_attributes: Mapping[str, str]
    include_edge_ends_as_properties: bool
    display_properties_line_wrap_limit: int
    # Default attributes for elements in the DOT graph.
    DEFAULT_GRAPH_ATTRIBUTES: ClassVar[Mapping[str, str]] = {
        "splines": "false",
        "bgcolor": DotColor.DARK_GRAY.value,
        # "nodesep": "3",
        # "ranksep": "3",
        # "rankdir": "LR",
        # "layout": "fdp",
        # "sep": "2",
    }

    DEFAULT_NODE_ATTRIBUTES: ClassVar[Mapping[str, str]] = {
        "fontcolor": DotColor.LIGHT_GRAY.value,
        "style": "filled",
        "color": DotColor.LIGHT_GRAY.value,
        "fillcolor": DotColor.EXTRA_DIM_GRAY.value,
        "fontname": "Courier New",
        "shape": "box",
    }

    DEFAULT_EDGE_ATTRIBUTES: ClassVar[Mapping[str, str]] = {
        "fontname": "Courier New",
        "fontsize": "10",
        "color": DotColor.LIGHT_GRAY.value,
        "fontcolor": DotColor.LIGHT_GRAY.value,
    }

    DEFAULT_CLUSTER_ATTRIBUTES: ClassVar[Mapping[str, str]] = {
        "fontname": "Courier New",
        "fontsize": "10",
        "fontcolor": DotColor.LIGHT_GRAY.value,
        "color": DotColor.LIGHT_GRAY.value,
        "pencolor": DotColor.BLACK.value,
        "penwidth": "3",
    }

    DEFAULT_EDGE_AS_NODE_ATTRIBUTES: ClassVar[Mapping[str, str]] = {
        "fontcolor": DotColor.LIGHT_GRAY.value,
        "style": "filled",
        "color": DotColor.TRANSPARENT.value,
        "fillcolor": DotColor.TRANSPARENT.value,
        "fontname": "Courier New",
        "shape": "box",
        "fontsize": "10",
        "margin": "0.0,0.0",
        "width": "0.0",
        "height": "0.0",
    }

    @staticmethod
    def create(
        include_graphical_attributes: bool = True,
        default_graph_attributes: Optional[Mapping[str, str]] = DEFAULT_GRAPH_ATTRIBUTES,
        default_cluster_attributes: Optional[Mapping[str, str]] = DEFAULT_CLUSTER_ATTRIBUTES,
        default_node_attributes: Optional[Mapping[str, str]] = DEFAULT_NODE_ATTRIBUTES,
        default_edge_attributes: Optional[Mapping[str, str]] = DEFAULT_EDGE_ATTRIBUTES,
        default_edge_as_node_attributes: Optional[Mapping[str, str]] = DEFAULT_EDGE_AS_NODE_ATTRIBUTES,
        include_edge_ends_as_properties: bool = True,
        display_properties_line_wrap_limit: int = 40,
    ) -> GraphicalDotConversionArgumentSet:
        return GraphicalDotConversionArgumentSet(
            include_graphical_attributes=include_graphical_attributes,
            default_cluster_attributes=mf_ensure_mapping(default_cluster_attributes),
            default_graph_attributes=mf_ensure_mapping(default_graph_attributes),
            default_node_attributes=mf_ensure_mapping(default_node_attributes),
            default_edge_attributes=mf_ensure_mapping(default_edge_attributes),
            default_edge_as_node_attributes=mf_ensure_mapping(default_edge_as_node_attributes),
            include_edge_ends_as_properties=include_edge_ends_as_properties,
            display_properties_line_wrap_limit=display_properties_line_wrap_limit,
        )


class MetricflowGraphToGraphicalDotConverter(MetricflowGraphConverter[DotGraphConversionResult]):
    def __init__(
        self,
        arguments: GraphicalDotConversionArgumentSet = GraphicalDotConversionArgumentSet.create(),
    ) -> None:
        self._arguments = arguments
        self._verbose_debug_logs = False

    @override
    def convert_graph(self, graph: MetricflowGraph) -> DotGraphConversionResult:
        dot_elements = self._create_dot_element_set(graph)
        return MetricflowGraphToDotConverter.convert_dot_elements(self._arguments, dot_elements)

    def _create_dot_element_set(self, graph: MetricflowGraph) -> DotGraphElementSet:
        cluster_name_to_dot_nodes: DefaultDict[Optional[str], list[DotNodeAttributeSet]] = defaultdict(list)
        for cluster_name, nodes in mf_group_by(graph.nodes, key=lambda n: n.node_descriptor.cluster_name):
            sorted_nodes = sorted(nodes)
            for node in sorted_nodes:
                dot_node = node.as_dot_node(include_graphical_attributes=self._arguments.include_graphical_attributes)
                dot_node = dot_node.with_attributes(label=self._make_node_label(node))
                cluster_name_to_dot_nodes[cluster_name].append(dot_node)

        edge_to_edge_as_dot_node_mapping: dict[MetricflowGraphEdge, DotNodeAttributeSet] = {}
        edge_as_node_index = 0
        for edge in graph.edges:
            edge_displayed_properties = list(edge.displayed_properties)

            if self._arguments.include_edge_ends_as_properties:
                edge_displayed_properties.extend(
                    (
                        DisplayedProperty("tail", edge.tail_node.node_descriptor.node_name),
                        DisplayedProperty("head", edge.head_node.node_descriptor.node_name),
                    )
                )

            if len(edge_displayed_properties) > 0:
                table_builder = GraphvizHtmlTableBuilder()
                self._add_displayed_properties_to_label_table(
                    table_builder=table_builder,
                    displayed_properties=edge_displayed_properties,
                )
                dot_node = DotNodeAttributeSet.create(
                    name=f"edge_as_node_{edge_as_node_index}",
                    label=table_builder.build(),
                    shape=DotNodeShape.PLAIN,
                    fill_color=DotColor.TRANSPARENT,
                )
                edge_to_edge_as_dot_node_mapping[edge] = dot_node
                edge_as_node_index += 1

                tail_node_cluster_name = edge.tail_node.node_descriptor.cluster_name
                head_node_cluster_name = edge.head_node.node_descriptor.cluster_name
                edge_as_dot_node_cluster_name = (
                    tail_node_cluster_name if tail_node_cluster_name == head_node_cluster_name else None
                )
                cluster_name_to_dot_nodes[edge_as_dot_node_cluster_name].append(dot_node)

        dot_edges: list[DotEdgeAttributeSet] = []
        for edge in graph.edges:
            if edge in edge_to_edge_as_dot_node_mapping:
                edge_dot_node = edge_to_edge_as_dot_node_mapping[edge]
                dot_edges.append(
                    DotEdgeAttributeSet.create(
                        tail_name=edge.tail_node.node_descriptor.node_name,
                        head_name=edge_dot_node.name,
                        arrow_shape=DotEdgeArrowShape.NONE,
                    )
                )
                dot_edges.append(
                    DotEdgeAttributeSet.create(
                        tail_name=edge_dot_node.name,
                        head_name=edge.head_node.node_descriptor.node_name,
                    )
                )
            else:
                dot_edges.append(
                    DotEdgeAttributeSet.create(
                        tail_name=edge.tail_node.node_descriptor.node_name,
                        head_name=edge.head_node.node_descriptor.node_name,
                    )
                )

        return DotGraphElementSet(
            dot_graph=graph.as_dot_graph(self._arguments.include_graphical_attributes),
            cluster_name_to_dot_nodes={
                cluster_name: tuple(dot_nodes) for cluster_name, dot_nodes in cluster_name_to_dot_nodes.items()
            },
            dot_edges=tuple(dot_edges),
        )

    def _make_node_label(self, node: MetricflowGraphNode) -> str:
        """Return a `graphviz` / DOT label to render this node.

        See https://graphviz.org/doc/info/shapes.html#html for HTML formatting.

        Currently, this return a `graphviz` HTML table where the first row is 1 column with the ID of
        the node, and subsequent row show the properties returned by `MetricflowGraphNode.displayed_properties`.
        """
        table_builder = GraphvizHtmlTableBuilder(table_border=0)

        with table_builder.new_row_builder() as row_builder:
            row_builder.add_column(
                GraphvizHtmlText(node.node_descriptor.node_name, style=GraphvizHtmlTextStyle.TITLE),
                alignment=GraphvizHtmlAlignment.CENTER,
                # column_span=2,
                cell_padding=4,
            )

        self._add_displayed_properties_to_label_table(table_builder, node.displayed_properties)

        result = table_builder.build()
        return result

    def _make_edge_label(self, displayed_properties: Iterable[DisplayedProperty]) -> str:
        """Similar to `_make_node_label()` but for edges"""
        table_builder = GraphvizHtmlTableBuilder(table_border=1)
        self._add_displayed_properties_to_label_table(table_builder, displayed_properties)
        return "\n".join(table_builder.build())

    def _add_displayed_properties_to_label_table(
        self,
        table_builder: GraphvizHtmlTableBuilder,
        displayed_properties: Iterable[DisplayedProperty],
        line_wrap_width: int = 40,
    ) -> None:
        for displayed_property in displayed_properties:
            key = displayed_property.key

            wrapped_lines = []
            if displayed_property.value is not None:
                for value_line in str(mf_first_non_none(displayed_property.value)).split("\n"):
                    wrapped_lines.extend(textwrap.wrap(value_line, width=line_wrap_width))

            # <BR> seems to have odd formatting issues, so using multiple rows to display mult-line text.
            if len(wrapped_lines) == 0:
                with table_builder.new_row_builder() as row_builder:
                    row_builder.add_column(
                        GraphvizHtmlText(
                            f"{key}",
                            GraphvizHtmlTextStyle.DESCRIPTION,
                        ),
                        alignment=GraphvizHtmlAlignment.LEFT,
                    )

            if len(wrapped_lines) == 1:
                with table_builder.new_row_builder() as row_builder:
                    row_builder.add_column(
                        GraphvizHtmlText(
                            f"{key}: {wrapped_lines[0]}",
                            GraphvizHtmlTextStyle.DESCRIPTION,
                        ),
                        alignment=GraphvizHtmlAlignment.LEFT,
                    )
                continue

            # If there are multiple lines in the value, generate multiple rows to indent lines. Output will look like:
            #
            #   example_key:
            #     A long
            #     value string.
            for row_index, value_line in enumerate(wrapped_lines):
                if row_index == 0:
                    with table_builder.new_row_builder() as row_builder:
                        row_builder.add_column(
                            GraphvizHtmlText(f"{key}:", style=GraphvizHtmlTextStyle.DESCRIPTION),
                            alignment=GraphvizHtmlAlignment.LEFT,
                        )
                    with table_builder.new_row_builder() as row_builder:
                        row_builder.add_column(
                            GraphvizHtmlText(mf_indent(value_line), style=GraphvizHtmlTextStyle.DESCRIPTION),
                            alignment=GraphvizHtmlAlignment.LEFT,
                        )
                else:
                    with table_builder.new_row_builder() as row_builder:
                        row_builder.add_column(
                            GraphvizHtmlText(
                                mf_indent(value_line),
                                GraphvizHtmlTextStyle.DESCRIPTION,
                            ),
                            alignment=GraphvizHtmlAlignment.LEFT,
                        )
