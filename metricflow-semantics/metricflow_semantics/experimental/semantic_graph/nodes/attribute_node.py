from __future__ import annotations

from abc import ABC
from functools import cached_property
from typing import ClassVar

from typing_extensions import override

from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
from metricflow_semantics.experimental.mf_graph.displayable_graph_element import MetricflowGraphLabel
from metricflow_semantics.experimental.mf_graph.formatting.graphviz_attributes import (
    DotNodeAttributeSet,
    GraphvizColor,
)
from metricflow_semantics.experimental.mf_graph.node_descriptor import MetricflowGraphNodeDescriptor
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    DunderNameElementLabel,
    GroupByAttributeLabel,
    MeasureAttributeLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphNode,
    SemanticGraphNodeVisitor,
)
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass
from metricflow_semantics.visitor import VisitorOutputT


@singleton_dataclass(order=False)
class AttributeNode(SemanticGraphNode, ABC):
    attribute_name: str

    @override
    def accept(self, visitor: SemanticGraphNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_attribute_node(self)

    @cached_property
    def comparison_key(self) -> ComparisonKey:
        return (self.attribute_name,)

    @override
    @property
    def dot_attributes(self) -> DotNodeAttributeSet:
        return super(SemanticGraphNode, self).dot_attributes.merge(
            DotNodeAttributeSet.create(
                color=GraphvizColor.SALMON_PINK,
            )
        )

    @property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(node_name=f"{self.__class__.__name__}({self.attribute_name})")

    @override
    @cached_property
    def dunder_name_element_label(self) -> DunderNameElementLabel:
        return DunderNameElementLabel(element_name=self.attribute_name)

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(SemanticGraphNode, self).labels.union((GroupByAttributeLabel(),))


@singleton_dataclass(order=False)
class TimeAttributeNode(AttributeNode):
    @staticmethod
    def get_instance(time_grain_name: str) -> TimeAttributeNode:
        return TimeAttributeNode(attribute_name=time_grain_name)

    @property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"Time({self.attribute_name})", cluster_name="time_attribute"
        )


@singleton_dataclass(order=False)
class MeasureAttributeNode(AttributeNode):
    model_id: SemanticModelId

    _labels: ClassVar[FrozenOrderedSet[MetricflowGraphLabel]] = FrozenOrderedSet((MeasureAttributeLabel(),))

    @property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"Measure({self.attribute_name})", cluster_name=self.model_id.cluster_name
        )

    @override
    @property
    def dot_attributes(self) -> DotNodeAttributeSet:
        return super(SemanticGraphNode, self).dot_attributes.merge(
            DotNodeAttributeSet.create(
                color=GraphvizColor.LIME_GREEN,
            )
        )

    @override
    @property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return self._labels


@singleton_dataclass(order=False)
class DsiEntityKeyAttributeNode(AttributeNode):
    @property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"DsiEntityKey({self.attribute_name})",
            cluster_name="other_attribute",
        )


@singleton_dataclass(order=False)
class CategoricalDimensionAttributeNode(AttributeNode):
    @property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"Dim({self.attribute_name})", cluster_name="other_attribute"
        )
