from __future__ import annotations

import logging
from functools import cached_property
from typing import Optional

from typing_extensions import override

from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
from metricflow_semantics.experimental.orderd_enum import OrderedEnum
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty

logger = logging.getLogger(__name__)


class EntityRelationship(OrderedEnum):
    LEFT_CARDINALITY_ONE = "left_cardinality_one"
    LEFT_CARDINALITY_MANY = "left_cardinality_many"
    RIGHT_CARDINALITY_ONE = "right_cardinality_one"
    RIGHT_CARDINALITY_MANY = "right_cardinality_many"
    VALID = "valid"

    @cached_property
    def inverse(self) -> EntityRelationship:
        # TODO: See if we need the different cardinality types.
        return self


@singleton_dataclass(order=False)
class EntityRelationshipEdge(SemanticGraphEdge):
    relationship: EntityRelationship
    linkable_element_properties: FrozenOrderedSet[LinkableElementProperty]

    @staticmethod
    def get_instance(
        tail_node: SemanticGraphNode,
        relationship: EntityRelationship,
        head_node: SemanticGraphNode,
        weight: Optional[int],
        linkable_element_properties: FrozenOrderedSet[LinkableElementProperty] = FrozenOrderedSet(),
    ) -> EntityRelationshipEdge:
        return EntityRelationshipEdge(
            _tail_node=tail_node,
            _head_node=head_node,
            _weight=weight,
            relationship=relationship,
            linkable_element_properties=linkable_element_properties,
        )

    @override
    @cached_property
    def comparison_key(self) -> ComparisonKey:
        return (self._tail_node, self._head_node, self.relationship, tuple(self.linkable_element_properties))

    @override
    @cached_property
    def inverse(self) -> EntityRelationshipEdge:
        return EntityRelationshipEdge.get_instance(
            tail_node=self._head_node,
            relationship=self.relationship.inverse,
            head_node=self._tail_node,
            linkable_element_properties=self.linkable_element_properties,
            weight=self._weight,
        )
