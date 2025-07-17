from __future__ import annotations

import logging
from functools import cached_property
from typing import Optional

from typing_extensions import override

from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.orderd_enum import OrderedEnum
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import AttributeRecipeUpdate
from metricflow_semantics.experimental.semantic_graph.edges.edge_labels import MetricDefinitionLabel
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass
from metricflow_semantics.mf_logging.pretty_formatter import PrettyFormatContext

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
    _recipe_update: AttributeRecipeUpdate

    @staticmethod
    def get_instance(
        tail_node: SemanticGraphNode,
        head_node: SemanticGraphNode,
        relationship: EntityRelationship = EntityRelationship.VALID,
        recipe_update: AttributeRecipeUpdate = AttributeRecipeUpdate(),
    ) -> EntityRelationshipEdge:
        return EntityRelationshipEdge(
            _tail_node=tail_node,
            _head_node=head_node,
            relationship=relationship,
            _recipe_update=recipe_update,
        )

    @override
    @cached_property
    def comparison_key(self) -> ComparisonKey:
        return (self._tail_node, self._head_node, self.relationship, self._recipe_update)

    @override
    @cached_property
    def inverse(self) -> EntityRelationshipEdge:
        return EntityRelationshipEdge.get_instance(
            tail_node=self._head_node,
            relationship=self.relationship.inverse,
            head_node=self._tail_node,
            recipe_update=self._recipe_update,
        )

    @override
    @property
    def recipe_update(self) -> AttributeRecipeUpdate:
        return self._recipe_update


_DEFAULT_ADDITIONAL_LABELS: FrozenOrderedSet[MetricflowGraphLabel] = FrozenOrderedSet()
_DEFAULT_RECIPE_UPDATE = AttributeRecipeUpdate()


@singleton_dataclass(order=False)
class MetricDefinitionEdge(SemanticGraphEdge):
    additional_labels: FrozenOrderedSet[MetricflowGraphLabel]
    _recipe_update: AttributeRecipeUpdate

    @staticmethod
    def get_instance(
        tail_node: SemanticGraphNode,
        head_node: SemanticGraphNode,
        additional_labels: Optional[FrozenOrderedSet[MetricflowGraphLabel]] = None,
        recipe_update: Optional[AttributeRecipeUpdate] = None,
    ) -> MetricDefinitionEdge:
        return MetricDefinitionEdge(
            _tail_node=tail_node,
            _head_node=head_node,
            additional_labels=additional_labels if additional_labels is not None else _DEFAULT_ADDITIONAL_LABELS,
            _recipe_update=recipe_update if recipe_update is not None else _DEFAULT_RECIPE_UPDATE,
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
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return self.additional_labels.union((MetricDefinitionLabel.get_instance(),))

    @property
    def recipe_update(self) -> AttributeRecipeUpdate:
        return self._recipe_update

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        formatter = format_context.formatter
        return formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping={
                "tail_node": self._tail_node,
                "head_node": self._head_node,
                "labels": self.labels,
                "recipe_update": self.recipe_update,
            },
        )
