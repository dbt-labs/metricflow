from __future__ import annotations

import logging
import typing
from typing import Optional

from typing_extensions import override

from metricflow_semantics.errors.error_classes import GraphvizException
from metricflow_semantics.toolkit.mf_graph.formatting.graph_formatter import MetricFlowGraphFormatter
from metricflow_semantics.toolkit.mf_graph.formatting.mf_to_graphical_dot import (
    MetricFlowGraphToGraphicalDotConverter,
)
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

if typing.TYPE_CHECKING:
    from metricflow_semantics.toolkit.mf_graph.mf_graph import (
        MetricFlowGraph,
    )

logger = logging.getLogger(__name__)


class SvgFormatter(MetricFlowGraphFormatter):
    """Format a graph as an SVG that can be displayed in a browser."""

    def __init__(self, converter: Optional[MetricFlowGraphToGraphicalDotConverter] = None) -> None:  # noqa: D107
        self._mf_to_dot_graph_converter = converter or MetricFlowGraphToGraphicalDotConverter()
        self._verbose_debug_logs = False

    @override
    def format_graph(self, graph: MetricFlowGraph) -> str:
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
