from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Tuple

from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.experimental.mf_graph.graph_element_id import GraphElementId
from metricflow_semantics.experimental.semantic_graph.graph_edges import SemanticGraphEdge
from metricflow_semantics.experimental.semantic_graph.graph_nodes import SemanticGraphNode


@dataclass(frozen=True)
class SemanticGraph:
    """Graph that represents the relationship between entities in the semantic manifest."""

    graph_id: GraphElementId
    nodes: Tuple[SemanticGraphNode, ...]
    edges: Tuple[SemanticGraphEdge, ...]

    @staticmethod
    def create(nodes: Iterable[SemanticGraphNode], edges: Iterable[SemanticGraphEdge]) -> SemanticGraph:  # noqa: D102
        return SemanticGraph(
            graph_id=GraphElementId.create_unique(StaticIdPrefix.SEMANTIC_GRAPH),
            nodes=tuple(sorted(nodes)),
            edges=tuple(sorted(edges)),
        )

    # def join(
    #     self, nodes: Optional[Iterable[SemanticGraphNode]] = None, edges: Optional[Iterable[SemanticGraphEdge]] = None
    # ) -> SemanticGraph:
    #     return SemanticGraph.create(
    #         nodes=set(self.nodes).union(nodes or set()), edges=set(self.edges).union(edges or set())
    #     )

    # @property
    # def nodes(self) -> Sequence[SemanticGraphNode]:
    #     return self._nodes
    #
    # @property
    # def edges(self) -> Sequence[SemanticGraphEdge]:
    #     return self._edges

    # def edges_with_tail_node(self, tail_node: SemanticGraphNode) -> Sequence[SemanticGraphEdge]:
    #     return tuple(edge for edge in self._edges if edge.tail_node == tail_node)
    #
    # def contains_node(self, target_node: SemanticGraphNode) -> bool:
    #     return target_node in self.node_set
    #
    # def contains_nodes(self, target_node_set: FrozenSet[SemanticGraphNode]) -> bool:
    #     return len(target_node_set.difference(self.node_set)) == 0
    #
    # @property
    # def node_set(self) -> FrozenSet[SemanticGraphNode]:
    #     return frozenset(self.nodes)
    #
    # @property
    # def edge_set(self) -> FrozenSet[SemanticGraphEdge]:
    #     return frozenset(self.edges)
