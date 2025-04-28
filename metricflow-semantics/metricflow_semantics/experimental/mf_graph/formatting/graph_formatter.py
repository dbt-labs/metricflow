from __future__ import annotations

import logging
import typing
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

if typing.TYPE_CHECKING:
    from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraph

logger = logging.getLogger(__name__)

OutputT = TypeVar("OutputT")


class GraphFormatter(Generic[OutputT], ABC):
    @abstractmethod
    def format_graph(self, graph: MetricflowGraph) -> OutputT:
        raise NotImplementedError
