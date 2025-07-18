from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Generic, Optional

from typing_extensions import override

from metricflow_semantics.experimental.mf_graph.mutable_graph import EdgeT, NodeT
from metricflow_semantics.experimental.mf_graph.path_finding.graph_path import MutablePathT

logger = logging.getLogger(__name__)


class WeightFunction(Generic[NodeT, EdgeT, MutablePathT], ABC):
    @abstractmethod
    def incremental_weight(self, path_to_node: MutablePathT, next_edge: EdgeT, max_path_weight: int) -> Optional[int]:
        raise NotImplementedError()


class EdgeCountWeightFunction(Generic[NodeT, EdgeT, MutablePathT], WeightFunction[NodeT, EdgeT, MutablePathT]):
    @override
    def incremental_weight(self, path_to_node: MutablePathT, next_edge: EdgeT, max_path_weight: int) -> Optional[int]:
        return 1
