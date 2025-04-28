from __future__ import annotations

from typing import Iterable

from metricflow_semantics.experimental.mf_graph.mf_graph import (
    MetricflowGraph,
    MetricflowGraphEdge,
    MetricflowGraphNode,
)
from metricflow_semantics.helpers.string_helpers import mf_indent


class GraphTextFormatter:
    """Formats a graph to text."""

    def _dot_node_section(self, nodes: Iterable[MetricflowGraphNode]) -> str:
        return "\n".join(node.dot_label for node in nodes)

    def _dot_edge_section(self, edges: Iterable[MetricflowGraphEdge]) -> str:
        lines = []
        for edge in edges:
            edge_label = edge.dot_label
            if "\n" in edge_label:
                lines.append(f'{edge.tail_node.dot_label} -> {edge.head_node.dot_label} [label="')
                lines.append(mf_indent(edge_label))
                lines.append('"]')
            else:
                lines.append(f'{edge.tail_node.dot_label} -> {edge.head_node.dot_label} [label="{edge.dot_label}"]')
        return "\n".join(lines)

    def dot_format(self, graph: MetricflowGraph) -> str:
        """Returns the graph in DOT notation format."""
        lines = [
            f"graph {graph.dot_label} {{",
            mf_indent(self._dot_node_section(sorted(graph.nodes))),
            mf_indent(self._dot_edge_section(sorted(graph.edges))),
            "}",
        ]
        return "\n".join(lines)
