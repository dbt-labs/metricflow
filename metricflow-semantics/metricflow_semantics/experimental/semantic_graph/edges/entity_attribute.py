from __future__ import annotations

import logging
from functools import cached_property
from typing import Optional

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from typing_extensions import override

from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
from metricflow_semantics.experimental.orderd_enum import OrderedEnum
from metricflow_semantics.experimental.semantic_graph.attribute_computation import (
    AttributeRecipeUpdate,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass

logger = logging.getLogger(__name__)


class AttributeEdgeType(OrderedEnum):
    ATTRIBUTE_TO_ENTITY = "attribute_to_entity"
    ENTITY_TO_ATTRIBUTE = "entity_to_attribute"

    @property
    def inverse(self) -> AttributeEdgeType:
        if self is AttributeEdgeType.ENTITY_TO_ATTRIBUTE:
            return AttributeEdgeType.ATTRIBUTE_TO_ENTITY
        elif self is AttributeEdgeType.ATTRIBUTE_TO_ENTITY:
            return AttributeEdgeType.ENTITY_TO_ATTRIBUTE
        else:
            assert_values_exhausted(self)


@singleton_dataclass(order=False)
class EntityAttributeEdge(SemanticGraphEdge):
    attribute_edge_type: AttributeEdgeType
    _recipe_update: AttributeRecipeUpdate

    @staticmethod
    def get_instance(
        tail_node: SemanticGraphNode,
        head_node: SemanticGraphNode,
        attribute_edge_type: AttributeEdgeType,
        attribute_recipe_update: Optional[AttributeRecipeUpdate] = None,
    ) -> EntityAttributeEdge:
        return EntityAttributeEdge(
            _tail_node=tail_node,
            _head_node=head_node,
            attribute_edge_type=attribute_edge_type,
            _recipe_update=attribute_recipe_update or AttributeRecipeUpdate(),
        )

    @override
    @cached_property
    def comparison_key(self) -> ComparisonKey:
        return (self._tail_node, self._head_node, self.attribute_edge_type, self._recipe_update)

    @override
    @cached_property
    def inverse(self) -> EntityAttributeEdge:
        return EntityAttributeEdge.get_instance(
            tail_node=self._head_node,
            head_node=self._tail_node,
            attribute_recipe_update=self._recipe_update,
            attribute_edge_type=self.attribute_edge_type.inverse,
        )

    @override
    @property
    def recipe_update(self) -> AttributeRecipeUpdate:
        return self._recipe_update
