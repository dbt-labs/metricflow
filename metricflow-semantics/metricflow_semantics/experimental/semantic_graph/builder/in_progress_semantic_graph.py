from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, FrozenSet, List, Optional, Sequence, Set

from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.experimental.mf_graph.graph_element_id import GraphElementId
from metricflow_semantics.experimental.semantic_graph.graph_edges import (
    ProvidedEdgeTagSet,
    RequiredTagSet,
    SemanticGraphEdge,
    SemanticGraphEdgeType,
)
from metricflow_semantics.experimental.semantic_graph.graph_nodes import AttributeNode, EntityNode, SemanticGraphNode
from metricflow_semantics.experimental.semantic_graph.graph_path.path_property import SemanticModelJoinOperation
from metricflow_semantics.experimental.semantic_graph.ids.attribute_ids import MetricAttributeId
from metricflow_semantics.experimental.semantic_graph.ids.entity_ids import (
    AssociativeEntityId,
    ConfiguredEntityId,
    TimeGrainEntityId,
)
from metricflow_semantics.experimental.semantic_graph.ids.node_id import SemanticGraphNodeId
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
class EntityIdSet:
    time_grain_ids: List[TimeGrainEntityId]


@dataclass
class InProgressSemanticGraph:
    """A mutable form of `SemanticGraph` that makes it easier to construct."""

    graph_id: GraphElementId
    _nodes: Set[SemanticGraphNode]
    _edges: Set[SemanticGraphEdge]

    _id_to_node: Dict[SemanticGraphNodeId, SemanticGraphNode]
    _id_to_metric_attribute_node: Dict[MetricAttributeId, AttributeNode]
    _id_to_entity_for_semantic_manifest_entity_node: Dict[ConfiguredEntityId, EntityNode]
    _id_to_composite_entity_node: Dict[AssociativeEntityId, EntityNode]

    @property
    def nodes(self) -> FrozenSet[SemanticGraphNode]:
        return frozenset(self._nodes)

    @property
    def edges(self) -> FrozenSet[SemanticGraphEdge]:
        return frozenset(self._edges)

    @staticmethod
    def create() -> InProgressSemanticGraph:  # noqa: D102
        return InProgressSemanticGraph(
            graph_id=GraphElementId.create_unique(StaticIdPrefix.SEMANTIC_GRAPH),
            _nodes=set(),
            _edges=set(),
            _id_to_node={},
            _id_to_metric_attribute_node={},
            _id_to_entity_for_semantic_manifest_entity_node={},
            _id_to_composite_entity_node={},
        )

    def _add_node(self, node: SemanticGraphNode) -> None:
        self._nodes.add(node)
        self._id_to_node[node.node_id] = node

    def add_metric_attribute_node(self, attribute_id: MetricAttributeId, node: AttributeNode) -> None:
        self._add_node(node)
        self._id_to_metric_attribute_node[attribute_id] = node

    def get_metric_attribute_ids(self) -> FrozenSet[MetricAttributeId]:
        return frozenset(self._id_to_metric_attribute_node.keys())

    def get_by_node_id(self, node_id: SemanticGraphNodeId) -> Optional[SemanticGraphNode]:
        return self._id_to_node.get(node_id)

    def contains_node(self, node: SemanticGraphNode) -> bool:
        return node in self._nodes

    def add_entity_node_for_semantic_manifest_entity(self, entity_id: ConfiguredEntityId, node: EntityNode) -> None:
        self._nodes.add(node)
        self._id_to_entity_for_semantic_manifest_entity_node[entity_id] = node

    def add_composite_entity_node(self, entity_id: AssociativeEntityId, node: EntityNode) -> None:
        self._nodes.add(node)
        self._id_to_composite_entity_node[entity_id] = node

    def add_node(self, node: SemanticGraphNode) -> None:
        self._add_node(node)

    def add_edge(
        self,
        tail_node: SemanticGraphNode,
        head_node: SemanticGraphNode,
        join_operations: Sequence[SemanticModelJoinOperation],
        required_tags: RequiredTagSet = RequiredTagSet.empty_set(),
        provided_tags: ProvidedEdgeTagSet = ProvidedEdgeTagSet.empty_set(),
    ) -> None:
        self._nodes.add(tail_node)
        self._nodes.add(head_node)

        edge = SemanticGraphEdge(
            tail_node=tail_node,
            head_node=head_node,
            join_operations=tuple(join_operations),
            required_tags=required_tags,
            provided_tags=provided_tags,
        )
        if edge in self._edges:
            logger.debug(LazyFormat("Not adding edge since it already exists.", edge=edge))
            return

        self._edges.add(edge)
        logger.debug(LazyFormat("Added edge.", edge=edge))

    def as_semantic_graph(self) -> SemanticGraph:
        return SemanticGraph.create(
            nodes=self._nodes,
            edges=tuple(
                SemanticGraphEdge(
                    tail_node=in_progress_edge.tail_node,
                    head_node=in_progress_edge.head_node,
                    join_operations=in_progress_edge.join_operations,
                    provided_tags=in_progress_edge.provided_tags,
                    required_tags=in_progress_edge.required_tags,
                )
                for in_progress_edge in self._edges
            ),
        )


@dataclass(frozen=True)
class _EdgeKey:
    """Key for deduplication of edges in the semantic graph."""

    tail_node: SemanticGraphNode
    edge_type: SemanticGraphEdgeType
    head_node: SemanticGraphNode
