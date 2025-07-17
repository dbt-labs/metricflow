from __future__ import annotations

import logging
from functools import cached_property

from typing_extensions import override

from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import AttributeRecipeUpdate
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass

logger = logging.getLogger(__name__)


@singleton_dataclass(order=False)
class JoinedDsiEntityEdge(SemanticGraphEdge):
    model_id: SemanticModelId

    @staticmethod
    def get_instance(
        tail_node: SemanticGraphNode,
        head_node: SemanticGraphNode,
        model_id: SemanticModelId,
    ) -> JoinedDsiEntityEdge:
        return JoinedDsiEntityEdge(
            _tail_node=tail_node,
            _head_node=head_node,
            model_id=model_id,
        )

    @override
    @property
    def comparison_key(self) -> ComparisonKey:
        return (self._tail_node, self._head_node, self.model_id)

    # @override
    # @property
    # def inverse(self) -> SemanticGraphEdge:
    #     raise NotImplementedError()

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet()

    @override
    @cached_property
    def inverse(self) -> JoinedDsiEntityEdge:
        return JoinedDsiEntityEdge.get_instance(
            tail_node=self._head_node,
            head_node=self._tail_node,
            model_id=self.model_id,
        )

    @override
    @property
    def recipe_update(self) -> AttributeRecipeUpdate:
        return AttributeRecipeUpdate(
            join_model=self.model_id,
        )
