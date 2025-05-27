from __future__ import annotations

import typing

from metricflow_semantics.experimental.mf_graph.formatting.graph_formatter import (
    MetricflowGraphFormatter,
)
from typing_extensions import override

from tests_metricflow_semantics.experimental.mf_graph.formatting.mf_to_dot import (
    MetricflowGraphToDotConverter,
)

if typing.TYPE_CHECKING:
    from metricflow_semantics.experimental.mf_graph.mf_graph import (
        MetricflowGraph,
    )


class DotNotationFormatter(MetricflowGraphFormatter):
    """Formats a graph to DOT notation (see https://graphviz.org/doc/info/lang.html)."""

    def __init__(self) -> None:
        super().__init__()
        self._mf_to_dot_graph_converter = MetricflowGraphToDotConverter()

    @override
    def format_graph(self, graph: MetricflowGraph) -> str:
        result = self._mf_to_dot_graph_converter.convert_graph(graph)
        return result.dot_graph.source
