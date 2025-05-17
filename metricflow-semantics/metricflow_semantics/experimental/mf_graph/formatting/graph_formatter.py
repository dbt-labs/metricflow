from __future__ import annotations

import logging
import typing
from abc import ABC, abstractmethod

if typing.TYPE_CHECKING:
    from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraph

logger = logging.getLogger(__name__)


class MetricflowGraphFormatter(ABC):
    """Interface for a graph-to-text formatter."""

    @abstractmethod
    def format_graph(self, graph: MetricflowGraph) -> str:
        """Format the given graph to text."""
        raise NotImplementedError
