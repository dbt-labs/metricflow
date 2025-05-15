from __future__ import annotations

from typing import override

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.entity_id import EntityId
from metricflow_semantics.experimental.semantic_graph.nodes.node_properties import (
    SemanticGraphProperty,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphNode,
    SemanticGraphNodeVisitor,
)
from metricflow_semantics.experimental.singleton import Singleton
from metricflow_semantics.visitor import VisitorOutputT


@fast_frozen_dataclass(order=False)
class EntityNode(SemanticGraphNode, Singleton):
    entity_id: EntityId
    _properties: FrozenOrderedSet[SemanticGraphProperty]

    @override
    def accept(self, visitor: SemanticGraphNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_entity_node(self)

    @property
    @override
    def dot_label(self) -> str:
        return self.entity_id.dot_label

    @property
    @override
    def comparison_key(self) -> tuple[EntityId]:
        return (self.entity_id,)

    @classmethod
    def get_instance(
        cls, entity_id: EntityId, entity_link_name: str, properties: OrderedSet[SemanticGraphProperty]
    ) -> EntityNode:  # noqa: D102
        return cls._get_singleton_by_kwargs(
            entity_id=entity_id,
            entity_link_name=entity_link_name,
            _properties=properties.as_frozen(),
        )
