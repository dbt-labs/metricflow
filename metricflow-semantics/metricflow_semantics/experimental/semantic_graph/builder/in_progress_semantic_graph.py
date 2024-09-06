from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Set

from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.experimental.mf_graph.graph_element_id import GraphElementId
from metricflow_semantics.experimental.semantic_graph.computation_method import ComputationMethod
from metricflow_semantics.experimental.semantic_graph.graph_edges import (
    ProvidedEdgeTagSet,
    RequiredTagSet,
    SemanticGraphEdge,
    SemanticGraphEdgeType,
)
from metricflow_semantics.experimental.semantic_graph.graph_nodes import SemanticGraphNode
from metricflow_semantics.experimental.semantic_graph.semantic_graph import SemanticGraph
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


# @dataclass
# class InProgressSemanticGraphEdge:
#     tail_node: SemanticGraphNode
#     edge_type: SemanticGraphEdgeType
#     head_node: SemanticGraphNode
#     computation_method: ComputationMethod
#
#     provided_tags: ProvidedEdgeTagSet
#     required_tags: RequiredTagSet


@dataclass
class InProgressSemanticGraph:
    """A mutable form of `SemanticGraph` that makes it easier to construct."""

    graph_id: GraphElementId
    nodes: Set[SemanticGraphNode]
    edges: Set[SemanticGraphEdge]

    @staticmethod
    def create() -> InProgressSemanticGraph:  # noqa: D102
        return InProgressSemanticGraph(
            graph_id=GraphElementId.create_unique(StaticIdPrefix.SEMANTIC_GRAPH),
            nodes=set(),
            edges=set(),
        )

    def add_edge(
        self,
        tail_node: SemanticGraphNode,
        edge_type: SemanticGraphEdgeType,
        head_node: SemanticGraphNode,
        computation_method: ComputationMethod,
        required_tags: RequiredTagSet,
        provided_tags: ProvidedEdgeTagSet = ProvidedEdgeTagSet.empty_set(),
    ) -> None:
        self.nodes.add(tail_node)
        self.nodes.add(head_node)

        edge = SemanticGraphEdge(
            tail_node=tail_node,
            edge_type=edge_type,
            head_node=head_node,
            computation_method=computation_method,
            required_tags=required_tags,
            provided_tags=provided_tags,
        )
        if edge in self.edges:
            logger.debug(LazyFormat("Not adding edge since it already exists.", edge=edge))
            return

        self.edges.add(edge)
        logger.debug(LazyFormat("Added edge.", edge=edge))

    def as_semantic_graph(self) -> SemanticGraph:
        return SemanticGraph.create(
            nodes=self.nodes,
            edges=tuple(
                SemanticGraphEdge(
                    tail_node=in_progress_edge.tail_node,
                    edge_type=in_progress_edge.edge_type,
                    head_node=in_progress_edge.head_node,
                    computation_method=in_progress_edge.computation_method,
                    provided_tags=in_progress_edge.provided_tags,
                    required_tags=in_progress_edge.required_tags,
                )
                for in_progress_edge in self.edges
            ),
        )


@dataclass(frozen=True)
class _EdgeKey:
    """Key for deduplication of edges in the semantic graph."""

    tail_node: SemanticGraphNode
    edge_type: SemanticGraphEdgeType
    head_node: SemanticGraphNode
