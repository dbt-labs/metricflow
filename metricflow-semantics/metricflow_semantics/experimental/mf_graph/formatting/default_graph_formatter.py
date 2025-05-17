from __future__ import annotations

import logging
import typing

from metricflow_semantics.mf_logging.pretty_print import mf_pformat

logger = logging.getLogger(__name__)

from metricflow_semantics.experimental.mf_graph.formatting.graph_formatter import MetricflowGraphFormatter

if typing.TYPE_CHECKING:
    from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraph


class DefaultGraphFormatter(MetricflowGraphFormatter):
    def format_graph(self, graph: MetricflowGraph) -> str:
        return mf_pformat(graph)
