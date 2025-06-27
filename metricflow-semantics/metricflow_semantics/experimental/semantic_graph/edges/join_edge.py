from __future__ import annotations

import logging
from functools import cached_property

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from typing_extensions import override

from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
from metricflow_semantics.experimental.orderd_enum import OrderedEnum
from metricflow_semantics.experimental.semantic_graph.attribute_computation import AttributeComputationUpdate
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
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
    source_model: SemanticModelId

    @staticmethod
    def get_instance(
        tail_node: SemanticGraphNode,
        head_node: SemanticGraphNode,
        left_model_id: SemanticModelId,
    ) -> JoinToModelEdge:
        return JoinToModelEdge(
            _tail_node=tail_node,
            _head_node=head_node,
            source_model=left_model_id,
        )

    @override
    @cached_property
    def comparison_key(self) -> ComparisonKey:
        return (self._tail_node, self._head_node, self.source_model)

    @override
    @cached_property
    def inverse(self) -> JoinToModelEdge:
        return JoinToModelEdge(
            _tail_node=self._tail_node,
            _head_node=self._head_node,
            source_model=self.source_model,
        )


@singleton_dataclass(order=False)
class JoinFromModelEdge(SemanticGraphEdge):
    right_model_id: SemanticModelId
    _attribute_computation_update: AttributeComputationUpdate
    @staticmethod
    def get_instance(
        tail_node: SemanticGraphNode,
        head_node: SemanticGraphNode,
        right_model_id: SemanticModelId,
        attribute_computation_update: AttributeComputationUpdate,
    ) -> JoinFromModelEdge:
        return JoinFromModelEdge(
            _tail_node=tail_node,
            _head_node=head_node,
            right_model_id=right_model_id,
            _attribute_computation_update=attribute_computation_update,
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
            right_model_id=self.right_model_id,
        )
