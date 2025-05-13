from __future__ import annotations

from typing import Iterable

from typing_extensions import override

from metricflow_semantics.experimental.mf_graph.formatting.graph_formatter import GraphFormatter
from metricflow_semantics.experimental.mf_graph.mf_graph import (
    MetricflowGraph,
    MetricflowGraphEdge,
    MetricflowGraphNode,
)
from metricflow_semantics.helpers.string_helpers import mf_indent


class DotNotationFormatter(GraphFormatter):
    """Formats a graph to DOT notation (see https://graphviz.org/doc/info/lang.html)."""

    def _dot_node_section(self, nodes: Iterable[MetricflowGraphNode]) -> str:
        return "\n".join(node.dot_label for node in nodes)

    def _dot_edge_section(self, edges: Iterable[MetricflowGraphEdge]) -> str:
        lines = []
        for edge in edges:
            lines.append(f"{edge.tail_node.dot_label} -> {edge.head_node.dot_label} [label={edge.dot_label!r}]")
        return "\n".join(lines)

    @override
    def format_graph(self, graph: MetricflowGraph) -> str:
        """Returns the graph in DOT notation format."""
        lines = [
            "graph {",
        ]
        inner_lines = (
            f"label={graph.dot_label!r}",
            self._dot_node_section(sorted(graph.nodes())),
            self._dot_edge_section(sorted(graph.edges())),
        )
        lines.extend(mf_indent(inner_line) for inner_line in inner_lines)
        lines.append("}")
        return "\n".join(lines)
