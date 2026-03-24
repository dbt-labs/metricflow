from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Generic, Optional

from typing_extensions import override

from metricflow_semantics.toolkit.mf_graph.mutable_graph import EdgeT, NodeT
from metricflow_semantics.toolkit.mf_graph.path_finding.graph_path import MutablePathT

logger = logging.getLogger(__name__)


class WeightFunction(Generic[NodeT, EdgeT, MutablePathT], ABC):
    """The weight function that is used to limit paths during graph traversal."""

    @abstractmethod
    def incremental_weight(self, path_to_node: MutablePathT, next_edge: EdgeT) -> Optional[int]:
        """Return the incremental weight added by adding the given edge to the path."""
        raise NotImplementedError()


class EdgeCountWeightFunction(Generic[NodeT, EdgeT, MutablePathT], WeightFunction[NodeT, EdgeT, MutablePathT]):
    """Translates the number of edges in the path to the weight."""

    @override
    def incremental_weight(self, path_to_node: MutablePathT, next_edge: EdgeT) -> Optional[int]:
        return 1
