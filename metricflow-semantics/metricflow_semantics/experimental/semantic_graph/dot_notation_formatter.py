from __future__ import annotations

from typing import Sequence

from metricflow_semantics.experimental.semantic_graph.graph_edges import SemanticGraphEdge
from metricflow_semantics.experimental.semantic_graph.graph_nodes import SemanticGraphNode
from metricflow_semantics.experimental.semantic_graph.semantic_graph import SemanticGraph
from metricflow_semantics.mf_logging.formatting import indent


class DotNotationFormatter:
    """Formats a semantic graph using DOT notation."""

    def _node_section(self, nodes: Sequence[SemanticGraphNode]) -> str:
        return "\n".join(node.dot_label for node in nodes)

    def _edge_section(self, edges: Sequence[SemanticGraphEdge]) -> str:
        lines = []
        for edge in edges:
            edge_label = edge.dot_label
            if "\n" in edge_label:
                lines.append(f'{edge.tail_node.dot_label} -> {edge.head_node.dot_label} [label="')
                lines.append(indent(edge_label))
                lines.append('"]')
            else:
                lines.append(f'{edge.tail_node.dot_label} -> {edge.head_node.dot_label} [label="{edge.dot_label}"]')
        return "\n".join(lines)

    def dot_format(self, semantic_graph: SemanticGraph) -> str:
        lines = [
            f"graph {semantic_graph.graph_id.str_value} {{",
            indent(self._node_section(semantic_graph.nodes)),
            indent(self._edge_section(semantic_graph.edges)),
            "}",
        ]
        return "\n".join(lines)
