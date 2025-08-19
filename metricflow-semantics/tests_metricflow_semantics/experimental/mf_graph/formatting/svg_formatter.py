from __future__ import annotations

import logging
import typing
from typing import Optional

from metricflow_semantics.experimental.metricflow_exception import GraphvizException
from metricflow_semantics.experimental.mf_graph.formatting.graph_formatter import MetricflowGraphFormatter
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

from tests_metricflow_semantics.experimental.mf_graph.formatting.mf_to_graphical_dot import (
    MetricflowGraphToGraphicalDotConverter,
)

if typing.TYPE_CHECKING:
    from metricflow_semantics.experimental.mf_graph.mf_graph import (
        MetricflowGraph,
    )

logger = logging.getLogger(__name__)


class SvgFormatter(MetricflowGraphFormatter):
    """Format a graph as an SVG that can be displayed in a browser."""

    def __init__(self, converter: Optional[MetricflowGraphToGraphicalDotConverter] = None) -> None:  # noqa: D107
        self._mf_to_dot_graph_converter = converter or MetricflowGraphToGraphicalDotConverter()
        self._verbose_debug_logs = False

    @override
    def format_graph(self, graph: MetricflowGraph) -> str:
        """Format the graph to the SVG image format using `graphviz`."""
        result = self._mf_to_dot_graph_converter.convert_graph(graph)
        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Converted graph to graphical DOT",
                    dot_source=result.dot_graph.source,
                    dot_elements=result.dot_element_set,
                )
            )

        try:
            return result.dot_graph.pipe(format="svg").decode("utf-8")
        except Exception as e:
            raise GraphvizException(
                LazyFormat(
                    "Error generating SVG",
                    result=result,
                )
            ) from e
