from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from metricflow_semantics.toolkit.mf_graph.mf_graph import MetricFlowGraph

logger = logging.getLogger(__name__)


OutputGraphT = TypeVar("OutputGraphT")


class MetricFlowGraphConverter(Generic[OutputGraphT], ABC):
    """Base class for a class that converts graphs."""

    @abstractmethod
    def convert_graph(self, graph: MetricFlowGraph) -> OutputGraphT:
        """Convert the graph to the given output type."""
        raise NotImplementedError()
