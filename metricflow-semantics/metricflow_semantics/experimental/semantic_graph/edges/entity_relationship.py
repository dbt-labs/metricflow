from __future__ import annotations

import logging
from functools import cached_property

from typing_extensions import override

from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.orderd_enum import OrderedEnum
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import AttributeRecipeUpdate
from metricflow_semantics.experimental.semantic_graph.edges.edge_labels import MetricDefinitionLabel
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass

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
    _attribute_computation_update: AttributeRecipeUpdate

    @staticmethod
    def get_instance(
        tail_node: SemanticGraphNode,
        head_node: SemanticGraphNode,
        relationship: EntityRelationship = EntityRelationship.VALID,
        attribute_computation_update: AttributeRecipeUpdate = AttributeRecipeUpdate(),
    ) -> EntityRelationshipEdge:
        return EntityRelationshipEdge(
            _tail_node=tail_node,
            _head_node=head_node,
            relationship=relationship,
            _attribute_computation_update=attribute_computation_update,
        )

    @override
    @cached_property
    def comparison_key(self) -> ComparisonKey:
        return (self._tail_node, self._head_node, self.relationship, self._attribute_computation_update)

    @override
    @cached_property
    def inverse(self) -> EntityRelationshipEdge:
        return EntityRelationshipEdge.get_instance(
            tail_node=self._head_node,
            relationship=self.relationship.inverse,
            head_node=self._tail_node,
            attribute_computation_update=self._attribute_computation_update,
        )

    @override
    @property
    def attribute_recipe_update(self) -> AttributeRecipeUpdate:
        return self._attribute_computation_update


@singleton_dataclass(order=False)
class MetricDefinitionEdge(SemanticGraphEdge):
    @staticmethod
    def get_instance(
        tail_node: SemanticGraphNode,
        head_node: SemanticGraphNode,
    ) -> MetricDefinitionEdge:
        return MetricDefinitionEdge(
            _tail_node=tail_node,
            _head_node=head_node,
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
    def inverse(self) -> MetricDefinitionEdge:
        return MetricDefinitionEdge.get_instance(
            tail_node=self._head_node,
            head_node=self._tail_node,
        )

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet((MetricDefinitionLabel.get_instance(),))
