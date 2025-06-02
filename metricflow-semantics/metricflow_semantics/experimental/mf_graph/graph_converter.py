from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraph

logger = logging.getLogger(__name__)


OutputGraphT = TypeVar("OutputGraphT")


class MetricflowGraphConverter(Generic[OutputGraphT], ABC):
    """Base class for a class that converts graphs."""

    @abstractmethod
    def convert_graph(self, graph: MetricflowGraph) -> OutputGraphT:
        """Convert the graph to the given output type."""
        raise NotImplementedError()
