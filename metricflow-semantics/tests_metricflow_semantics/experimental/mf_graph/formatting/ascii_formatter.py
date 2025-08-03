from __future__ import annotations

import logging
import typing

from metricflow_semantics.experimental.metricflow_exception import GraphvizException
from metricflow_semantics.experimental.mf_graph.formatting.graph_formatter import MetricflowGraphFormatter
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

from tests_metricflow_semantics.experimental.mf_graph.formatting.mf_to_dot import MetricflowGraphToDotConverter

if typing.TYPE_CHECKING:
    from metricflow_semantics.experimental.mf_graph.mf_graph import (
        MetricflowGraph,
    )

logger = logging.getLogger(__name__)


class AsciiFormatter(MetricflowGraphFormatter):
    """Format a graph as an SVG that can be displayed in a browser."""

    def __init__(self) -> None:  # noqa: D107
        self._mf_graph_to_dot_converter = MetricflowGraphToDotConverter()
        self._verbose_debug_logs = False

    @override
    def format_graph(self, graph: MetricflowGraph) -> str:
        """Format the graph to the SVG image format using `graphviz`."""
        conversion_result = self._mf_graph_to_dot_converter.convert_graph(graph)

        try:
            return conversion_result.dot_graph.pipe(format="ascii").decode("utf-8")
        except Exception as e:
            raise GraphvizException(
                LazyFormat(
                    "Error generating an ASCII representation of the graph",
                    graph=graph,
                    conversion_result=conversion_result,
                )
            ) from e
