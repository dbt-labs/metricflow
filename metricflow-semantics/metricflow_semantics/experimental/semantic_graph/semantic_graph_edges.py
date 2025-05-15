from __future__ import annotations

import logging

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraphEdge
from metricflow_semantics.experimental.semantic_graph.semantic_graph_nodes import SemanticGraphNode

logger = logging.getLogger(__name__)


@fast_frozen_dataclass(order=False)
class SemanticGraphEdge(MetricflowGraphEdge[SemanticGraphNode]):
    @property
    def dot_label(self) -> str:
        return f"{self._tail_node.dot_label}->{self._head_node.dot_label}"
