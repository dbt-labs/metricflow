from __future__ import annotations

import logging
import typing
from abc import ABC, abstractmethod

if typing.TYPE_CHECKING:
    from metricflow_semantics.toolkit.mf_graph.mf_graph import MetricFlowGraph

logger = logging.getLogger(__name__)


class MetricFlowGraphFormatter(ABC):
    """Interface for a graph-to-text formatter."""

    @abstractmethod
    def format_graph(self, graph: MetricFlowGraph) -> str:
        """Format the given graph to text."""
        raise NotImplementedError
