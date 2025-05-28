from __future__ import annotations

import logging
import typing
from collections import defaultdict
from collections.abc import Mapping
from typing import DefaultDict, Iterable, Optional

from metricflow_semantics.collection_helpers.syntactic_sugar import mf_group_by
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.experimental.mf_graph.formatting.dot_attributes import (
    DotColor,
    DotEdgeArrowShape,
    DotEdgeAttributeSet,
    DotNodeAttributeSet,
    DotNodeShape,
)
from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraphEdge, MetricflowGraphNode, NodeT

from tests_metricflow_semantics.experimental.mf_graph.formatting.graphviz_html import (
    GraphvizHtmlAlignment,
    GraphvizHtmlText,
    GraphvizHtmlTextStyle,
)
from tests_metricflow_semantics.experimental.mf_graph.formatting.graphviz_html_table_builder import (
    GraphvizHtmlTableBuilder,
)
from tests_metricflow_semantics.experimental.mf_graph.formatting.mf_to_dot import (
    DotGraphElementSet,
    MetricflowGraphToDotConverter,
    ToDotConverterParameterSet,
)

if typing.TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class MetricflowGraphToGraphicalDotGraphConverter(MetricflowGraphToDotConverter):
    # Default attributes for elements in the DOT graph.
    DEFAULT_GRAPH_ATTRIBUTES: Mapping[str, str] = {
        "splines": "false",
        "bgcolor": DotColor.DARK_GRAY.value,
        # "nodesep": "3",
        # "ranksep": "3",
        # "rankdir": "LR",
        # "layout": "fdp",
        # "sep": "2",
    }

    DEFAULT_NODE_ATTRIBUTES: Mapping[str, str] = {
        "fontcolor": DotColor.LIGHT_GRAY.value,
        "style": "filled",
        "color": DotColor.LIGHT_GRAY.value,
        "fillcolor": DotColor.EXTRA_DIM_GRAY.value,
        "fontname": "Courier New",
        "shape": "box",
    }

    DEFAULT_EDGE_ATTRIBUTES: Mapping[str, str] = {
        "fontname": "Courier New",
        "fontsize": "10",
        "color": DotColor.LIGHT_GRAY.value,
        "fontcolor": DotColor.LIGHT_GRAY.value,
    }

    DEFAULT_CLUSTER_ATTRIBUTES: Mapping[str, str] = {
        "fontname": "Courier New",
        "fontsize": "10",
        "fontcolor": DotColor.LIGHT_GRAY.value,
        "color": DotColor.LIGHT_GRAY.value,
        "pencolor": DotColor.BLACK.value,
        "penwidth": "3",
    }

    DEFAULT_EDGE_AS_NODE_ATTRIBUTES: Mapping[str, str] = {
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

    _NODE_NAME_KEY = "name"
    _HEAD_NAME_KEY = "head_name"
    _TAIL_NAME_KEY = "tail_name"
    _ARROWHEAD_KEY = "arrowhead"
    _LABEL_KEY = "label"

    def __init__(
        self,
        parameter_set: ToDotConverterParameterSet = ToDotConverterParameterSet.create(
            include_graphical_attributes=True,
            default_graph_attributes=DEFAULT_GRAPH_ATTRIBUTES,
            default_cluster_attributes=DEFAULT_CLUSTER_ATTRIBUTES,
            default_node_attributes=DEFAULT_NODE_ATTRIBUTES,
            default_edge_attributes=DEFAULT_EDGE_ATTRIBUTES,
        ),
        default_edge_as_node_attributes: Mapping[str, str] = DEFAULT_EDGE_AS_NODE_ATTRIBUTES,
    ) -> None:
        super().__init__(parameter_set=parameter_set)
        self._default_edge_as_node_attributes = default_edge_as_node_attributes
        self._display_properties_line_wrap_limit = 40

    def _create_dot_element_set(
        self, nodes: Iterable[MetricflowGraphNode], edges: Iterable[MetricflowGraphEdge[NodeT]]
    ) -> DotGraphElementSet:
        cluster_name_to_dot_nodes: DefaultDict[Optional[str], list[DotNodeAttributeSet]] = defaultdict(list)
        for cluster_name, nodes in mf_group_by(nodes, key=lambda n: n.node_descriptor.cluster_name):
            sorted_nodes = sorted(nodes)
            for node in sorted_nodes:
                dot_node = node.as_dot_node(
                    include_graphical_attributes=self._parameter_set.include_graphical_attributes
                )
                dot_node = dot_node.with_attributes(label=self._make_node_label(node))
                cluster_name_to_dot_nodes[cluster_name].append(dot_node)

        edge_to_edge_as_dot_node_mapping: dict[MetricflowGraphEdge, DotNodeAttributeSet] = {}
        edge_as_node_index = 0
        for edge in edges:
            edge_displayed_properties = edge.displayed_properties
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
        for edge in edges:
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

        return DotGraphElementSet(cluster_name_to_dot_nodes=cluster_name_to_dot_nodes, dot_edges=dot_edges)

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
