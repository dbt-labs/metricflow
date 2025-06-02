from __future__ import annotations

import logging
import typing

from typing_extensions import override

from metricflow_semantics.experimental.mf_graph.formatting.graph_formatter import MetricflowGraphFormatter
from metricflow_semantics.mf_logging.pretty_print import mf_pformat

if typing.TYPE_CHECKING:
    from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraph

logger = logging.getLogger(__name__)


class PrettyFormatGraphFormatter(MetricflowGraphFormatter):
    """Formats a graph using `mf_pformat()`."""

    @override
    def format_graph(self, graph: MetricflowGraph) -> str:
        return mf_pformat(graph)
