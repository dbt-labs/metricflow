from __future__ import annotations

import logging
from functools import cached_property
from typing import Optional, override

from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME

from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
from metricflow_semantics.experimental.mf_graph.formatting.dot_attributes import (
    DotColor,
    DotNodeAttributeSet,
)
from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.mf_graph.node_descriptor import MetricflowGraphNodeDescriptor
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import (
    AttributeComputationUpdate,
)
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    AggregationLabel,
    DsiEntityLabel,
    DunderNameElementLabel,
    GroupByAttributeRootLabel,
    JoinFromLabel,
    JoinViaLabel,
    MetricTimeLabel,
    TimeDimensionLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphNode,
)
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType

logger = logging.getLogger(__name__)


@singleton_dataclass(order=False)
class DsiEntityNode(SemanticGraphNode):
    entity_name: str

    @staticmethod
    def get_instance(entity_name: str) -> DsiEntityNode:
        return DsiEntityNode(
            entity_name=entity_name,
        )

    @property
    @override
    def comparison_key(self) -> ComparisonKey:
        return (self.entity_name,)

    @override
    def as_dot_node(self, include_graphical_attributes: bool) -> DotNodeAttributeSet:
        dot_node = super(DsiEntityNode, self).as_dot_node(include_graphical_attributes)
        if include_graphical_attributes:
            dot_node = dot_node.with_attributes(color=DotColor.CORNFLOWER_BLUE)
        return dot_node

    @override
    @cached_property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"DsiEntity({self.entity_name})", cluster_name="model_entity"
        )

    @override
    @cached_property
    def dunder_name_element_label(self) -> DunderNameElementLabel:
        return DunderNameElementLabel(element_name=self.entity_name)

    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet((self.dunder_name_element_label, DsiEntityLabel()))

    @override
    @cached_property
    def attribute_computation_update(self) -> AttributeComputationUpdate:
        return AttributeComputationUpdate(dundered_name_element_addition=self.entity_name)


@singleton_dataclass(order=False)
class TimeBaseNode(SemanticGraphNode):
    time_grain_name: str

    @override
    @cached_property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"TimeBase({self.time_grain_name})", cluster_name="time_base"
        )

    @property
    def comparison_key(self) -> ComparisonKey:
        return (self.time_grain_name,)


@singleton_dataclass(order=False)
class AggregationNode(SemanticGraphNode):
    model_id: SemanticModelId
    aggregation_time_dimension_name: str

    @override
    @cached_property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"Agg({self.model_id.model_name}, {self.aggregation_time_dimension_name})",
            cluster_name=self.model_id.cluster_name,
        )

    @property
    def comparison_key(self) -> ComparisonKey:
        return (self.model_id.comparison_key, self.aggregation_time_dimension_name)

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(AggregationNode, self).labels.union((AggregationLabel(),))


@singleton_dataclass(order=False)
class JoinToModelNode(SemanticGraphNode):
    model_id: SemanticModelId

    @override
    @cached_property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"JoinTo({self.model_id.model_name})",
            cluster_name=self.model_id.cluster_name,
        )

    @property
    def comparison_key(self) -> ComparisonKey:
        return self.model_id.comparison_key

    @override
    @cached_property
    def attribute_computation_update(self) -> AttributeComputationUpdate:
        return AttributeComputationUpdate(
            derived_from_model_id_additions=(self.model_id,),
        )


@singleton_dataclass(order=False)
class JoinFromModelNode(SemanticGraphNode):
    model_id: SemanticModelId

    @override
    @cached_property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"JoinFrom({self.model_id})",
            cluster_name=self.model_id.cluster_name,
        )

    @override
    @property
    def comparison_key(self) -> ComparisonKey:
        return self.model_id.comparison_key

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(SemanticGraphNode, self).labels.union((JoinFromLabel(),))

    @override
    @property
    def attribute_computation_update(self) -> AttributeComputationUpdate:
        return AttributeComputationUpdate(
            derived_from_model_id_additions=(self.model_id,),
        )


@singleton_dataclass(order=False)
class TimeDimensionNode(SemanticGraphNode):
    dimension_name: str
    time_grain_name: str

    @override
    @cached_property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"TimeDimension({self.dimension_name}, {self.time_grain_name})",
            cluster_name="time_entity",
        )

    @property
    def comparison_key(self) -> ComparisonKey:
        return (self.dimension_name,)

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(TimeDimensionNode, self).labels.union((TimeDimensionLabel(),))

    @override
    @cached_property
    def dunder_name_element_label(self) -> Optional[DunderNameElementLabel]:
        return DunderNameElementLabel(element_name=self.dimension_name)

    @override
    @property
    def attribute_computation_update(self) -> AttributeComputationUpdate:
        return AttributeComputationUpdate(
            dundered_name_element_addition=self.dimension_name,
            element_type_addition=LinkableElementType.TIME_DIMENSION,
        )


@singleton_dataclass(order=False)
class MetricTimeDimensionNode(TimeDimensionNode):
    @staticmethod
    def get_instance(time_grain_name: str) -> MetricTimeDimensionNode:
        return MetricTimeDimensionNode(
            dimension_name=METRIC_TIME_ELEMENT_NAME,
            time_grain_name=time_grain_name,
        )

    @override
    @cached_property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"MetricTimeDimension({self.time_grain_name})", cluster_name="time_dimension"
        )

    @property
    def comparison_key(self) -> ComparisonKey:
        return (self.time_grain_name,)

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(MetricTimeDimensionNode, self).labels.union((MetricTimeLabel(),))

    @override
    @property
    def attribute_computation_update(self) -> AttributeComputationUpdate:
        return AttributeComputationUpdate(
            dundered_name_element_addition=self.dimension_name,
            linkable_element_property_additions=(LinkableElementProperty.METRIC_TIME,),
            element_type_addition=LinkableElementType.TIME_DIMENSION,
        )


@singleton_dataclass(order=False)
class GroupByAttributeRootNode(SemanticGraphNode):
    @override
    @override
    @property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name="GroupByAttributeRoot",
            cluster_name=None,
        )

    @override
    @property
    def comparison_key(self) -> ComparisonKey:
        return ()

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(GroupByAttributeRootNode, self).labels.union((GroupByAttributeRootLabel(),))

    @override
    def as_dot_node(self, include_graphical_attributes: bool) -> DotNodeAttributeSet:
        dot_node = super(GroupByAttributeRootNode, self).as_dot_node(include_graphical_attributes)
        if include_graphical_attributes:
            dot_node = dot_node.with_attributes(color=DotColor.GOLD)
        return dot_node


@singleton_dataclass(order=False)
class JoinViaModelNode(SemanticGraphNode):
    model_id: SemanticModelId

    @override
    @cached_property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"JoinVia({self.model_id})",
            cluster_name=self.model_id.cluster_name,
        )

    @override
    @property
    def comparison_key(self) -> ComparisonKey:
        return self.model_id.comparison_key

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(SemanticGraphNode, self).labels.union((JoinViaLabel(),))

    @override
    @property
    def attribute_computation_update(self) -> AttributeComputationUpdate:
        return AttributeComputationUpdate(
            derived_from_model_id_additions=(self.model_id,),
        )
