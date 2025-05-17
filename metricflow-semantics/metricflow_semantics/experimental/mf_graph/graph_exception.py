from __future__ import annotations

import logging
import typing

from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


from metricflow_semantics.experimental.metricflow_exception import MetricflowException

if typing.TYPE_CHECKING:
    from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraph, MetricflowGraphNode


class UnknownNodeException(MetricflowException):
    def __init__(
        self,
        node: MetricflowGraphNode,
        graph: MetricflowGraph,
    ) -> None:
        super().__init__(LazyFormat("Unknown node in the graph", node=node, nodes_in_graph=graph.nodes))
