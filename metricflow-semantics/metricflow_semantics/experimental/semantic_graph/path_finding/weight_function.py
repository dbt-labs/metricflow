from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Generic, Optional

from typing_extensions import override

from metricflow_semantics.experimental.mf_graph.mutable_graph import EdgeT, NodeT
from metricflow_semantics.experimental.semantic_graph.path_finding.graph_path import PathT

logger = logging.getLogger(__name__)


class WeightFunction(Generic[NodeT, EdgeT, PathT], ABC):
    @abstractmethod
    def incremental_weight(self, path_to_node: PathT, edge_from_node: EdgeT, path_weight_limit: int) -> Optional[int]:
        raise NotImplementedError()


class EdgeCountWeightFunction(Generic[NodeT, EdgeT, PathT], WeightFunction[NodeT, EdgeT, PathT]):
    @override
    def incremental_weight(self, path_to_node: PathT, edge_from_node: EdgeT, path_weight_limit: int) -> Optional[int]:
        return 1
