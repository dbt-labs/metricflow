from __future__ import annotations

import contextlib
import logging
import typing
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Optional

import graphviz
from graphviz import Digraph
from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.collection_helpers.syntactic_sugar import (
    mf_ensure_mapping,
    mf_group_by,
)
from metricflow_semantics.experimental.mf_graph.formatting.dot_attributes import (
    DotEdgeAttributeSet,
    DotGraphAttributeSet,
    DotNodeAttributeSet,
)
from metricflow_semantics.experimental.mf_graph.graph_converter import MetricflowGraphConverter
from typing_extensions import override

if typing.TYPE_CHECKING:
    from metricflow_semantics.experimental.mf_graph.mf_graph import (
        MetricflowGraph,
    )

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


@dataclass
class DotConversionArgumentSet:
    include_graphical_attributes: bool
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
    ) -> DotConversionArgumentSet:
        return DotConversionArgumentSet(
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
        arguments: DotConversionArgumentSet = DotConversionArgumentSet.create(),
    ) -> None:
        self._arguments = arguments

    def _create_dot_element_set(
        self,
        graph: MetricflowGraph,
    ) -> DotGraphElementSet:
        cluster_name_to_dot_nodes: dict[Optional[str], AnyLengthTuple[DotNodeAttributeSet]] = {}
        for cluster_name, nodes in mf_group_by(graph.nodes, key=lambda node: node.node_descriptor.cluster_name):
            cluster_name_to_dot_nodes[cluster_name] = tuple(
                node.as_dot_node(self._arguments.include_graphical_attributes) for node in sorted(nodes)
            )

        dot_graph = graph.as_dot_graph(self._arguments.include_graphical_attributes)
        dot_graph = DotGraphAttributeSet.create(
            name=dot_graph.name, additional_kwargs=self._arguments.default_graph_attributes
        ).merge(dot_graph)

        return DotGraphElementSet(
            dot_graph=dot_graph,
            cluster_name_to_dot_nodes=cluster_name_to_dot_nodes,
            dot_edges=tuple(edge.as_dot_edge(self._arguments.include_graphical_attributes) for edge in graph.edges),
        )

    @staticmethod
    def convert_dot_elements(
        converter_arguments: DotConversionArgumentSet, dot_element_set: DotGraphElementSet
    ) -> DotGraphConversionResult:
        dot_graph = DotGraphAttributeSet.create(
            name=dot_element_set.dot_graph.name, additional_kwargs=converter_arguments.default_graph_attributes
        ).merge(dot_element_set.dot_graph)

        dot = graphviz.Digraph(
            graph_attr=dot_graph.dot_graph_attrs,
            node_attr=converter_arguments.default_node_attributes,
            edge_attr=converter_arguments.default_node_attributes,
        )

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
                    subgraph.attr(label=cluster_name, **converter_arguments.default_cluster_attributes)

                for dot_node in dot_nodes:
                    subgraph.node(**dot_node.dot_kwargs)
                    all_dot_nodes.append(dot_node)

        for dot_edge in dot_element_set.dot_edges:
            dot.edge(**dot_edge.dot_kwargs)

        return DotGraphConversionResult(
            dot_graph=dot,
            dot_element_set=dot_element_set,
        )

    @override
    def convert_graph(self, graph: MetricflowGraph) -> DotGraphConversionResult:
        dot_elements = self._create_dot_element_set(graph)
        return MetricflowGraphToDotConverter.convert_dot_elements(self._arguments, dot_elements)


@dataclass
class DotGraphElementSet:
    dot_graph: DotGraphAttributeSet
    cluster_name_to_dot_nodes: Mapping[Optional[str], AnyLengthTuple[DotNodeAttributeSet]]
    dot_edges: AnyLengthTuple[DotEdgeAttributeSet]
