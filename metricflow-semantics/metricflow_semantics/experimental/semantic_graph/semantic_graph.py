from __future__ import annotations

import logging
from abc import ABC
from collections import defaultdict

from typing_extensions import override

from metricflow_semantics.experimental.mf_graph.formatting.dot_attributes import DotGraphAttributeSet
from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraph
from metricflow_semantics.experimental.mf_graph.mutable_graph import MutableGraph
from metricflow_semantics.experimental.ordered_set import MutableOrderedSet
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)

logger = logging.getLogger(__name__)


class SemanticGraph(MetricflowGraph[SemanticGraphNode, SemanticGraphEdge], ABC):
    pass


class MutableSemanticGraph(MutableGraph[SemanticGraphNode, SemanticGraphEdge], SemanticGraph):
    @staticmethod
    def create() -> MutableSemanticGraph:
        return MutableSemanticGraph(
            _nodes=MutableOrderedSet(),
            _edges=MutableOrderedSet(),
            _tail_node_to_edges=defaultdict(MutableOrderedSet),
            _head_node_to_edges=defaultdict(MutableOrderedSet),
            _label_to_nodes=defaultdict(MutableOrderedSet),
            _label_to_edges=defaultdict(MutableOrderedSet),
        )

    @staticmethod
    def create_copy(other: MutableSemanticGraph) -> MutableSemanticGraph:
        return MutableSemanticGraph(
            _nodes=other._nodes,
            _edges=MutableOrderedSet(),
            _tail_node_to_edges=defaultdict(MutableOrderedSet),
            _head_node_to_edges=defaultdict(MutableOrderedSet),
            _label_to_nodes=defaultdict(MutableOrderedSet),
            _label_to_edges=defaultdict(MutableOrderedSet),
        )

    @override
    def intersection(self, other: MetricflowGraph[SemanticGraphNode, SemanticGraphEdge]) -> MutableSemanticGraph:
        intersection_graph = MutableSemanticGraph.create()
        self.add_edges(self._intersect_edges(other))
        return intersection_graph

    @override
    def inverse(self) -> MutableSemanticGraph:
        inverse_graph = MutableSemanticGraph.create()
        for edge in self.edges:
            inverse_graph.add_edge(edge.inverse)
        return inverse_graph

    @override
    def as_dot_graph(self, include_graphical_attributes: bool) -> DotGraphAttributeSet:
        return (
            super()
            .as_dot_graph(include_graphical_attributes=include_graphical_attributes)
            .with_attributes(
                dot_kwargs={
                    "rankdir": "LR",
                }
            )
        )
