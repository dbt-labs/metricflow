from __future__ import annotations

from metricflow_semantics.experimental.mf_graph.formatting.graph_formatter import (
    MetricflowGraphFormatter,
)
from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraph
from tests_metricflow_semantics.experimental.mf_graph.formatting.mf_to_dot import (
    MetricflowGraphToDotConverter,
)
from typing_extensions import override


class DotNotationFormatter(MetricflowGraphFormatter):
    """Formats a graph to DOT notation (see https://graphviz.org/doc/info/lang.html)."""

    def __init__(self) -> None:  # noqa: D107
        super().__init__()
        self._mf_to_dot_graph_converter = MetricflowGraphToDotConverter()

    @override
    def format_graph(self, graph: MetricflowGraph) -> str:
        result = self._mf_to_dot_graph_converter.convert_graph(graph)
        return result.dot_graph.source
