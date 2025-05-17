from __future__ import annotations

import contextlib
import logging
import typing
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Optional

import graphviz
from graphviz import Digraph
from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.mf_graph.formatting.graphviz_attributes import (
    GraphvizColor,
)
from metricflow_semantics.experimental.mf_graph.graph_converter import MetricflowGraphConverter
from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraphNode

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
    node_kwarg_dicts: AnyLengthTuple[Mapping[str, str]]
    edge_kwarg_dicts: AnyLengthTuple[Mapping[str, str]]


class MetricflowGraphToDotGraphConverter(MetricflowGraphConverter[DotGraphConversionResult]):
    # Default attributes for elements in the DOT graph.
    DEFAULT_GRAPH_ATTRIBUTES: Mapping[str, str] = {
        "splines": "false",
        "bgcolor": GraphvizColor.DARK_GRAY.value,
    }

    DEFAULT_NODE_ATTRIBUTES: Mapping[str, str] = {
        "fontcolor": GraphvizColor.LIGHT_GRAY.value,
        "style": "filled",
        "color": GraphvizColor.LIGHT_GRAY.value,
        "fillcolor": GraphvizColor.EXTRA_DIM_GRAY.value,
        "fontname": "Courier New",
    }

    DEFAULT_EDGE_ATTRIBUTES: Mapping[str, str] = {
        "fontname": "Courier New",
        "fontsize": "10",
        "color": GraphvizColor.LIGHT_GRAY.value,
        "fontcolor": GraphvizColor.LIGHT_GRAY.value,
    }

    DEFAULT_CLUSTER_ATTRIBUTERS: Mapping[str, str] = {
        "fontname": "Courier New",
        "fontsize": "10",
        "fontcolor": GraphvizColor.LIGHT_GRAY.value,
        "color": GraphvizColor.LIGHT_GRAY.value,
        "pencolor": GraphvizColor.BLACK.value,
        "penwidth": "3",
    }

    def __init__(
        self,
        include_only_required_dot_attributes: bool,
        default_graph_attributes: Mapping[str, str] = DEFAULT_GRAPH_ATTRIBUTES,
        default_node_attributes: Mapping[str, str] = DEFAULT_NODE_ATTRIBUTES,
        default_edge_attributes: Mapping[str, str] = DEFAULT_EDGE_ATTRIBUTES,
    ) -> None:
        self._include_only_required_dot_attributes = include_only_required_dot_attributes
        self._default_graph_attributes = default_graph_attributes
        self._default_node_attributes = default_node_attributes
        self._default_edge_attributes = default_edge_attributes

    def convert_graph(self, graph: MetricflowGraph) -> DotGraphConversionResult:
        dot = graphviz.Digraph(
            name=graph.__class__.__name__,
            graph_attr=self._default_graph_attributes if not self._include_only_required_dot_attributes else {},
            node_attr=self._default_node_attributes if not self._include_only_required_dot_attributes else {},
            edge_attr=self._default_node_attributes if not self._include_only_required_dot_attributes else {},
        )
        # Group nodes by the cluster name for visualization.
        cluster_name_to_nodes: dict[Optional[str], list[MetricflowGraphNode]] = defaultdict(list)
        # Record the args use to create the nodes and edges for debug context.
        node_kwarg_dicts = []
        edge_kwarg_dicts = []

        # If the node has a `cluster_name`, add the nodes in a DOT subgraph so that they can be
        # displayed as a group.
        for node in sorted(graph.nodes):
            cluster_name_to_nodes[node.node_descriptor.cluster_name].append(node)

        for cluster_name, nodes in cluster_name_to_nodes.items():
            subgraph_context = (
                # Graphviz needs the subgraph to be prefixed with the constant.
                dot.subgraph(name="cluster_" + cluster_name)
                if cluster_name is not None
                else contextlib.nullcontext(enter_result=dot)
            )

            with subgraph_context as subgraph:
                if cluster_name is not None and not self._include_only_required_dot_attributes:
                    subgraph.attr(label=cluster_name, **MetricflowGraphToDotGraphConverter.DEFAULT_CLUSTER_ATTRIBUTERS)

                for node in nodes:
                    node_kwarg_dict = node.dot_attributes.as_kwargs()
                    if self._include_only_required_dot_attributes:
                        required_keys = {"name"}
                        node_kwarg_dict = {key: value for key, value in node_kwarg_dict.items() if key in required_keys}

                    node_kwarg_dicts.append(node_kwarg_dict)
                    subgraph.node(**node_kwarg_dict)

        for edge in sorted(graph.edges):
            edge_kwarg_set = edge.dot_attributes.as_kwargs()
            if self._include_only_required_dot_attributes:
                required_keys = {"tail_name", "head_name"}
                edge_kwarg_set = {key: value for key, value in edge_kwarg_set.items() if key in required_keys}

            dot.edge(**edge_kwarg_set)
            edge_kwarg_dicts.append(edge_kwarg_set)

        return DotGraphConversionResult(
            dot_graph=dot,
            node_kwarg_dicts=tuple(node_kwarg_dicts),
            edge_kwarg_dicts=tuple(edge_kwarg_dicts),
        )
