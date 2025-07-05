from __future__ import annotations

import logging
from functools import cached_property

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from typing_extensions import override

from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
from metricflow_semantics.experimental.orderd_enum import OrderedEnum
from metricflow_semantics.experimental.semantic_graph.attribute_computation import AttributeRecipeUpdate
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
class JoinToModelEdge(SemanticGraphEdge):
    # joined_model: SemanticModelId

    @staticmethod
    def get_instance(
        tail_node: SemanticGraphNode,
        head_node: SemanticGraphNode,
        # joined_model: SemanticModelId,
    ) -> JoinToModelEdge:
        return JoinToModelEdge(
            _tail_node=tail_node,
            _head_node=head_node,
            # joined_model=joined_model,
        )

    @override
    @cached_property
    def comparison_key(self) -> ComparisonKey:
        # return (self._tail_node, self._head_node, self.joined_model)
        return (
            self._tail_node,
            self._head_node,
        )

    @override
    @cached_property
    def inverse(self) -> JoinToModelEdge:
        return JoinToModelEdge.get_instance(
            tail_node=self._tail_node,
            head_node=self._head_node,
            # joined_model=self.joined_model,
        )

    # @override
    # @cached_property
    # def attribute_computation_update(self) -> AttributeComputationUpdate:
    #     return AttributeComputationUpdate(derived_from_model_id_additions=(self.joined_model,))


@singleton_dataclass(order=False)
class JoinFromModelEdge(SemanticGraphEdge):
    _recipe_update: AttributeRecipeUpdate

    @staticmethod
    def get_instance(
        tail_node: SemanticGraphNode,
        head_node: SemanticGraphNode,
        recipe_update: AttributeRecipeUpdate,
    ) -> JoinFromModelEdge:
        return JoinFromModelEdge(
            _tail_node=tail_node,
            _head_node=head_node,
            _recipe_update=recipe_update,
        )

    @override
    @cached_property
    def comparison_key(self) -> ComparisonKey:
        return (
            self._tail_node,
            self._head_node,
        )

    @override
    @cached_property
    def inverse(self) -> JoinFromModelEdge:
        return JoinFromModelEdge(
            _tail_node=self._tail_node,
            _head_node=self._head_node,
            _recipe_update=self._recipe_update,
        )

    @override
    @property
    def attribute_recipe_update(self) -> AttributeRecipeUpdate:
        return self._recipe_update
