from __future__ import annotations

import logging
import typing

from typing_extensions import override

from metricflow_semantics.toolkit.mf_graph.formatting.graph_formatter import MetricFlowGraphFormatter
from metricflow_semantics.toolkit.mf_logging.pretty_print import mf_pformat

if typing.TYPE_CHECKING:
    from metricflow_semantics.toolkit.mf_graph.mf_graph import MetricFlowGraph

logger = logging.getLogger(__name__)


class PrettyFormatGraphFormatter(MetricFlowGraphFormatter):
    """Formats a graph using `mf_pformat()`."""

    @override
    def format_graph(self, graph: MetricFlowGraph) -> str:
        return mf_pformat(graph)
