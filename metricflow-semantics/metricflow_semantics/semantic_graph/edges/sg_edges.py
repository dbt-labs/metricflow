from __future__ import annotations

import logging
from functools import cached_property
from typing import Optional

from typing_extensions import override

from metricflow_semantics.semantic_graph.attribute_resolution.attribute_recipe_step import (
    AttributeRecipeStep,
)
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.semantic_graph.sg_interfaces import SemanticGraphEdge, SemanticGraphNode
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, OrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_graph.comparable import ComparisonKey
from metricflow_semantics.toolkit.mf_graph.graph_labeling import MetricFlowGraphLabel
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import PrettyFormatContext

logger = logging.getLogger(__name__)


@fast_frozen_dataclass(order=False)
class JoinToModelEdge(SemanticGraphEdge):
    """Edge that describes joining a model on the right side."""

    right_model_id: SemanticModelId

    @staticmethod
    def create(  # noqa: D102
        tail_node: SemanticGraphNode,
        head_node: SemanticGraphNode,
        right_model_id: SemanticModelId,
    ) -> JoinToModelEdge:
        return JoinToModelEdge(
            tail_node=tail_node,
            head_node=head_node,
            right_model_id=right_model_id,
        )

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return (self.tail_node, self.head_node, self.right_model_id)

    @cached_property
    @override
    def inverse(self) -> JoinToModelEdge:
        return JoinToModelEdge.create(
            tail_node=self.tail_node,
            head_node=self.head_node,
            right_model_id=self.right_model_id,
        )

    @cached_property
    @override
    def recipe_step_to_append(self) -> AttributeRecipeStep:
        return AttributeRecipeStep(add_model_join=self.right_model_id)


_EMPTY_LABEL_SET: FrozenOrderedSet[MetricFlowGraphLabel] = FrozenOrderedSet()
_EMPTY_RECIPE_STEP = AttributeRecipeStep()


@fast_frozen_dataclass(order=False)
class MetricDefinitionEdge(SemanticGraphEdge):
    """An edge that points from a metric to the inputs for the metric."""

    additional_labels: FrozenOrderedSet[MetricFlowGraphLabel]
    _recipe_step: AttributeRecipeStep

    @staticmethod
    def create(  # noqa: D102
        tail_node: SemanticGraphNode,
        head_node: SemanticGraphNode,
        additional_labels: Optional[FrozenOrderedSet[MetricFlowGraphLabel]] = None,
        recipe_step: Optional[AttributeRecipeStep] = None,
    ) -> MetricDefinitionEdge:
        return MetricDefinitionEdge(
            tail_node=tail_node,
            head_node=head_node,
            additional_labels=additional_labels if additional_labels is not None else _EMPTY_LABEL_SET,
            _recipe_step=recipe_step if recipe_step is not None else _EMPTY_RECIPE_STEP,
        )

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return (
            self.tail_node,
            self.head_node,
        )

    @cached_property
    @override
    def inverse(self) -> MetricDefinitionEdge:
        return MetricDefinitionEdge.create(
            tail_node=self.head_node,
            head_node=self.tail_node,
        )

    @cached_property
    @override
    def recipe_step_to_append(self) -> AttributeRecipeStep:
        return self._recipe_step

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        formatter = format_context.formatter
        return formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping={
                "tail_node": self.tail_node,
                "head_node": self.head_node,
                "labels": self.labels,
                "recipe_step": self.recipe_step_to_append,
            },
        )

    @cached_property
    @override
    def labels(self) -> OrderedSet[MetricFlowGraphLabel]:
        return FrozenOrderedSet(self.additional_labels)


@fast_frozen_dataclass(order=False)
class EntityRelationshipEdge(SemanticGraphEdge):
    """Represents a relationship between entity nodes."""

    _recipe_update: AttributeRecipeStep

    @staticmethod
    def create(  # noqa: D102
        tail_node: SemanticGraphNode,
        head_node: SemanticGraphNode,
        recipe_update: AttributeRecipeStep = _EMPTY_RECIPE_STEP,
    ) -> EntityRelationshipEdge:
        return EntityRelationshipEdge(
            tail_node=tail_node,
            head_node=head_node,
            _recipe_update=recipe_update,
        )

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return (self.tail_node, self.head_node, self._recipe_update)

    @cached_property
    @override
    def inverse(self) -> EntityRelationshipEdge:
        return EntityRelationshipEdge.create(
            tail_node=self.head_node,
            head_node=self.tail_node,
            recipe_update=self._recipe_update,
        )

    @cached_property
    @override
    def recipe_step_to_append(self) -> AttributeRecipeStep:
        return self._recipe_update


@fast_frozen_dataclass(order=False)
class EntityAttributeEdge(SemanticGraphEdge):
    """Edge from entity nodes to attribute nodes."""

    _recipe_step: AttributeRecipeStep

    @staticmethod
    def create(  # noqa: D102
        tail_node: SemanticGraphNode,
        head_node: SemanticGraphNode,
        recipe_step: Optional[AttributeRecipeStep] = None,
    ) -> EntityAttributeEdge:
        return EntityAttributeEdge(
            tail_node=tail_node,
            head_node=head_node,
            _recipe_step=recipe_step or _EMPTY_RECIPE_STEP,
        )

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return (self.tail_node, self.head_node, self._recipe_step)

    @cached_property
    @override
    def inverse(self) -> EntityAttributeEdge:
        return EntityAttributeEdge.create(
            tail_node=self.head_node,
            head_node=self.tail_node,
            recipe_step=self._recipe_step,
        )

    @cached_property
    @override
    def recipe_step_to_append(self) -> AttributeRecipeStep:
        return self._recipe_step
