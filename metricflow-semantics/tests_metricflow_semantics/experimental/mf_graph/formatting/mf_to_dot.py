from __future__ import annotations

import contextlib
import logging
import textwrap
import typing
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import DefaultDict, Iterable, Optional, Sequence

import graphviz
from graphviz import Digraph
from metricflow_semantics.collection_helpers.syntactic_sugar import mf_ensure_mapping, mf_group_by
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.formatting.dot_attributes import (
    DotEdgeAttributeSet,
    DotNodeAttributeSet,
)
from tests_metricflow_semantics.experimental.mf_graph.formatting.graphviz_html import (
    GraphvizHtmlAlignment,
    GraphvizHtmlText,
    GraphvizHtmlTextStyle,
)
from tests_metricflow_semantics.experimental.mf_graph.formatting.graphviz_html_table_builder import GraphvizHtmlTableBuilder
from metricflow_semantics.experimental.mf_graph.graph_converter import MetricflowGraphConverter
from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraphEdge, MetricflowGraphNode
from metricflow_semantics.helpers.string_helpers import mf_indent

if typing.TYPE_CHECKING:
    from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraph

logger = logging.getLogger(__name__)


@dataclass
class DotGraphConversionResult:
    """The results of converting to a DOT graph.

    Since this is just used to pass data between calls, using a regular `dataclass` so that it can directly store a
    dict.
    """

    dot_graph: Digraph
    # For debugging context.
    dot_element_set: DotGraphElementSet


@fast_frozen_dataclass()
class ToDotConverterParameterSet:
    include_graphical_attributes: bool = False
    default_cluster_attributes: Mapping[str, str]
    default_graph_attributes: Mapping[str, str]
    default_node_attributes: Mapping[str, str]
    default_edge_attributes: Mapping[str, str]

    @staticmethod
    def create(
        include_graphical_attributes: bool = False,
        default_graph_attributes: Optional[Mapping[str, str]] = None,
        default_cluster_attributes: Optional[Mapping[str, str]] = None,
        default_node_attributes: Optional[Mapping[str, str]] = None,
        default_edge_attributes: Optional[Mapping[str, str]] = None,
    ) -> ToDotConverterParameterSet:
        return ToDotConverterParameterSet(
            include_graphical_attributes=include_graphical_attributes,
            default_cluster_attributes=mf_ensure_mapping(default_cluster_attributes),
            default_graph_attributes=mf_ensure_mapping(default_graph_attributes),
            default_node_attributes=mf_ensure_mapping(default_node_attributes),
            default_edge_attributes=mf_ensure_mapping(default_edge_attributes),
        )


class MetricflowGraphToDotConverter(MetricflowGraphConverter[DotGraphConversionResult]):
    # Default attributes for elements in the DOT graph.

    def __init__(
        self,
        parameter_set: ToDotConverterParameterSet = ToDotConverterParameterSet.create(),
    ) -> None:
        self._parameter_set = parameter_set

    def _create_dot_element_set(
        self, nodes: Iterable[MetricflowGraphNode], edges: Iterable[MetricflowGraphEdge]
    ) -> DotGraphElementSet:
        return DotGraphElementSet(
            cluster_name_to_dot_nodes=self._create_cluster_name_to_dot_node_mapping(nodes),
            dot_edges=self._create_dot_edges(edges),
        )

    def convert_graph(self, graph: MetricflowGraph) -> DotGraphConversionResult:
        graph_attributes = dict(self._parameter_set.default_graph_attributes)
        dot_graph = graph.as_dot_graph(self._parameter_set.include_graphical_attributes)
        graph_attributes["name"] = dot_graph.name
        graph_attributes.update(dot_graph.dot_graph_attrs)
        dot = graphviz.Digraph(
            graph_attr=graph_attributes,
            node_attr=self._parameter_set.default_node_attributes,
            edge_attr=self._parameter_set.default_node_attributes,
        )

        dot_element_set = self._create_dot_element_set(graph.nodes, graph.edges)

        all_dot_nodes: list[DotNodeAttributeSet] = []
        for cluster_name, dot_nodes in dot_element_set.cluster_name_to_dot_nodes.items():
            subgraph_context = (
                # Graphviz needs the subgraph to be prefixed with the constant.
                dot.subgraph(name="cluster_" + cluster_name)
                if cluster_name is not None
                else contextlib.nullcontext(enter_result=dot)
            )

            with subgraph_context as subgraph:
                if cluster_name is not None:
                    subgraph.attr(label=cluster_name, **self._parameter_set.default_cluster_attributes)

                for dot_node in dot_nodes:
                    subgraph.node(**dot_node.dot_kwargs)
                    all_dot_nodes.append(dot_node)

        for dot_edge in dot_element_set.dot_edges:
            dot.edge(**dot_edge.dot_kwargs)

        return DotGraphConversionResult(
            dot_graph=dot,
            dot_element_set=dot_element_set,
        )

    def _add_displayed_properties_to_label_table(
        self,
        table_builder: GraphvizHtmlTableBuilder,
        displayed_properties: Iterable[DisplayedProperty],
        word_wrap_limit: int = 30,
    ) -> None:
        for displayed_property in displayed_properties:
            key = displayed_property.key
            value_lines = textwrap.wrap(str(displayed_property.value), width=word_wrap_limit)
            # <BR> seems to have odd formatting issues, so using multiple rows to display mult-line text.
            if len(value_lines) == 0:
                continue

            if len(value_lines) == 1:
                with table_builder.new_row_builder() as row_builder:
                    row_builder.add_column(
                        GraphvizHtmlText(
                            f"{key}: {value_lines[0]}",
                            GraphvizHtmlTextStyle.DESCRIPTION,
                        ),
                        alignment=GraphvizHtmlAlignment.LEFT,
                    )
            else:
                for row_index, value_line in enumerate(value_lines):
                    with table_builder.new_row_builder() as row_builder:
                        if row_index == 0:
                            row_builder.add_column(
                                GraphvizHtmlText(f"{key}:", style=GraphvizHtmlTextStyle.DESCRIPTION),
                                alignment=GraphvizHtmlAlignment.LEFT,
                            )
                        else:
                            row_builder.add_column(
                                GraphvizHtmlText(
                                    mf_indent(value_line),
                                    GraphvizHtmlTextStyle.DESCRIPTION,
                                ),
                                alignment=GraphvizHtmlAlignment.LEFT,
                            )

    def _create_cluster_name_to_dot_node_mapping(
        self, nodes: Iterable[MetricflowGraphNode]
    ) -> DefaultDict[Optional[str], list[DotNodeAttributeSet]]:
        cluster_name_to_dot_nodes: DefaultDict[Optional[str], list[DotNodeAttributeSet]] = defaultdict(list)
        for cluster_name, nodes in mf_group_by(nodes, key=lambda node: node.node_descriptor.cluster_name):
            sorted_nodes = sorted(nodes)
            for ordered_node in sorted_nodes:
                cluster_name_to_dot_nodes[cluster_name].append(
                    ordered_node.as_dot_node(
                        include_graphical_attributes=self._parameter_set.include_graphical_attributes
                    )
                )
        return cluster_name_to_dot_nodes

    def _create_dot_edges(self, edges: Iterable[MetricflowGraphEdge]) -> list[DotEdgeAttributeSet]:
        return [
            edge.as_dot_edge(include_graphical_attributes=self._parameter_set.include_graphical_attributes)
            for edge in edges
        ]


@dataclass
class DotGraphElementSet:
    cluster_name_to_dot_nodes: Mapping[Optional[str], Sequence[DotNodeAttributeSet]]
    dot_edges: Sequence[DotEdgeAttributeSet]
