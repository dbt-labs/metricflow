from __future__ import annotations

import logging
from abc import ABC

from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraph
from metricflow_semantics.experimental.mf_graph.mutable_graph import MutableGraph
from metricflow_semantics.experimental.semantic_graph.edges.semantic_graph_edge import SemanticGraphEdge
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import SemanticGraphNode

logger = logging.getLogger(__name__)


class SemanticGraph(MetricflowGraph[SemanticGraphNode, SemanticGraphEdge], ABC):
    pass


class MutableSemanticGraph(MutableGraph[SemanticGraphNode, SemanticGraphEdge], SemanticGraph):
    @property
    def dot_label(self) -> str:
        return "semantic_graph"
