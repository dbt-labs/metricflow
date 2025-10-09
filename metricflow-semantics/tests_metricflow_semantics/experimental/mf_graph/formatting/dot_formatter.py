from __future__ import annotations

from metricflow_semantics.experimental.mf_graph.formatting.graph_formatter import (
    MetricFlowGraphFormatter,
)
from metricflow_semantics.experimental.mf_graph.mf_graph import MetricFlowGraph
from typing_extensions import override

from tests_metricflow_semantics.experimental.mf_graph.formatting.mf_to_dot import (
    MetricFlowGraphToDotConverter,
)


class DotNotationFormatter(MetricFlowGraphFormatter):
    """Formats a graph to DOT notation (see https://graphviz.org/doc/info/lang.html)."""

    def __init__(self) -> None:  # noqa: D107
        super().__init__()
        self._mf_to_dot_graph_converter = MetricFlowGraphToDotConverter()

    @override
    def format_graph(self, graph: MetricFlowGraph) -> str:
        result = self._mf_to_dot_graph_converter.convert_graph(graph)
        return result.dot_graph.source
