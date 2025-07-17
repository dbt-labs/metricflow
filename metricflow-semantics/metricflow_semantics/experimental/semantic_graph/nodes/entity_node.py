from __future__ import annotations

import logging
from abc import ABC
from functools import cached_property
from typing import override

from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME

from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
from metricflow_semantics.experimental.mf_graph.formatting.dot_attributes import (
    DotColor,
    DotNodeAttributeSet,
)
from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.mf_graph.node_descriptor import MetricflowGraphNodeDescriptor
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import (
    AttributeRecipeUpdate,
)
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    BaseMetricLabel,
    DerivedMetricLabel,
    DsiEntityLabel,
    JoinedModelLabel,
    KeyEntityClusterLabel,
    KeyEntityLabel,
    LocalModelLabel,
    MetricLabel,
    MetricTimeLabel,
    TimeClusterLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.sg_constant import ClusterName
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType

logger = logging.getLogger(__name__)


@singleton_dataclass(order=False)
class DsiEntityNode(SemanticGraphNode):
    entity_name: str
    model_id: SemanticModelId

    @staticmethod
    def get_instance(entity_name: str, model_id: SemanticModelId) -> DsiEntityNode:
        return DsiEntityNode(
            entity_name=entity_name,
            model_id=model_id,
        )

    @property
    @override
    def comparison_key(self) -> ComparisonKey:
        return (self.entity_name, self.model_id)

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
            node_name=f"{self.model_id}.{self.entity_name}",
            cluster_name=self.entity_name,
        )

    @cached_property
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet((DsiEntityLabel.get_instance(),))

    @override
    @cached_property
    def recipe_update(self) -> AttributeRecipeUpdate:
        return AttributeRecipeUpdate(
            add_entity_link=self.entity_name,
            add_dunder_name_element=self.entity_name,
            join_model=self.model_id,
        )


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
class JoinedModelNode(SemanticGraphNode):
    model_id: SemanticModelId

    @staticmethod
    def get_instance(model_id: SemanticModelId) -> JoinedModelNode:  # noqa: D102
        return JoinedModelNode(model_id=model_id)

    @override
    @cached_property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"JoinedModel({self.model_id})",
            cluster_name=self.model_id.cluster_name,
        )

    @override
    @property
    def comparison_key(self) -> ComparisonKey:
        return self.model_id.comparison_key

    @override
    @cached_property
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet((JoinedModelLabel.get_instance(),))

    @override
    @cached_property
    def recipe_update(self) -> AttributeRecipeUpdate:
        return AttributeRecipeUpdate(
            join_model=self.model_id,
        )


@singleton_dataclass(order=False)
class LocalModelNode(SemanticGraphNode):
    model_id: SemanticModelId

    @staticmethod
    def get_instance(model_id: SemanticModelId) -> LocalModelNode:  # noqa: D102
        return LocalModelNode(model_id=model_id)

    @override
    @cached_property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"LocalModel({self.model_id})",
            cluster_name=self.model_id.cluster_name,
        )

    @override
    @property
    def comparison_key(self) -> ComparisonKey:
        return self.model_id.comparison_key

    @override
    @cached_property
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet((LocalModelLabel.get_instance(),))

    @override
    @cached_property
    def recipe_update(self) -> AttributeRecipeUpdate:
        return AttributeRecipeUpdate(
            join_model=self.model_id,
        )


@singleton_dataclass(order=False)
class TimeDimensionNode(SemanticGraphNode):
    dimension_name: str

    @staticmethod
    def get_instance(dimension_name: str) -> TimeDimensionNode:
        return TimeDimensionNode(dimension_name=dimension_name)

    @override
    @cached_property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"TimeDimension({self.dimension_name})",
            cluster_name=ClusterName.TIME,
        )

    @property
    def comparison_key(self) -> ComparisonKey:
        return (self.dimension_name,)

    @override
    @cached_property
    def recipe_update(self) -> AttributeRecipeUpdate:
        return AttributeRecipeUpdate(
            add_dunder_name_element=self.dimension_name,
            set_element_type=LinkableElementType.TIME_DIMENSION,
        )


@singleton_dataclass(order=False)
class MetricTimeNode(SemanticGraphNode):
    @staticmethod
    def get_instance() -> MetricTimeNode:
        return MetricTimeNode()

    @override
    @cached_property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name="MetricTime",
            cluster_name=ClusterName.TIME,
        )

    @property
    def comparison_key(self) -> ComparisonKey:
        return ()

    @override
    @cached_property
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet((MetricTimeLabel.get_instance(), TimeClusterLabel.get_instance()))

    @override
    @cached_property
    def recipe_update(self) -> AttributeRecipeUpdate:
        return AttributeRecipeUpdate(
            add_dunder_name_element=METRIC_TIME_ELEMENT_NAME,
            add_properties=(LinkableElementProperty.METRIC_TIME,),
            set_element_type=LinkableElementType.TIME_DIMENSION,
        )


@singleton_dataclass(order=False)
class GroupByAttributeRootNode(SemanticGraphNode):
    @staticmethod
    def get_instance() -> GroupByAttributeRootNode:
        return GroupByAttributeRootNode()

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
    def as_dot_node(self, include_graphical_attributes: bool) -> DotNodeAttributeSet:
        dot_node = super(GroupByAttributeRootNode, self).as_dot_node(include_graphical_attributes)
        if include_graphical_attributes:
            dot_node = dot_node.with_attributes(color=DotColor.GOLD)
        return dot_node


@singleton_dataclass(order=False)
class TimeEntityNode(SemanticGraphNode):
    @staticmethod
    def get_instance() -> TimeEntityNode:
        return TimeEntityNode()

    @override
    @cached_property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name="TimeEntity",
            cluster_name=ClusterName.TIME,
        )

    @override
    @property
    def comparison_key(self) -> ComparisonKey:
        return ()

    @override
    @cached_property
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet((TimeClusterLabel.get_instance(),))


@singleton_dataclass(order=False)
class MetricNode(SemanticGraphNode, ABC):
    metric_name: str

    @override
    @property
    def comparison_key(self) -> ComparisonKey:
        return (self.metric_name,)

    @override
    @cached_property
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet((MetricLabel.get_instance(), MetricLabel.get_instance(self.metric_name)))

    @override
    @property
    def recipe_update(self) -> AttributeRecipeUpdate:
        return AttributeRecipeUpdate(
            add_dunder_name_element=self.metric_name,
        )


@singleton_dataclass(order=False)
class BaseMetricNode(MetricNode):
    @staticmethod
    def get_instance(metric_name: str) -> BaseMetricNode:  # noqa: D102
        return BaseMetricNode(metric_name=metric_name)

    @property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"BaseMetric({self.metric_name})", cluster_name=ClusterName.METRIC
        )

    @override
    @cached_property
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return super(BaseMetricNode, self).labels.union(
            (BaseMetricLabel.get_instance(), BaseMetricLabel.get_instance(self.metric_name))
        )


@singleton_dataclass(order=False)
class DerivedMetricNode(MetricNode):
    @staticmethod
    def get_instance(metric_name: str) -> DerivedMetricNode:  # noqa: D102
        return DerivedMetricNode(metric_name=metric_name)

    @property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"DerivedMetric({self.metric_name})", cluster_name=ClusterName.METRIC
        )

    @override
    @cached_property
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return super(DerivedMetricNode, self).labels.union(
            (DerivedMetricLabel.get_instance(), BaseMetricLabel.get_instance(self.metric_name))
        )


@singleton_dataclass(order=False)
class KeyEntityNode(SemanticGraphNode):
    dsi_entity_name: str

    @staticmethod
    def get_instance(dsi_entity_name: str) -> KeyEntityNode:  # noqa: D102
        return KeyEntityNode(dsi_entity_name=dsi_entity_name)

    @override
    @property
    def comparison_key(self) -> ComparisonKey:
        return (self.dsi_entity_name,)

    @property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"KeyEntity({self.dsi_entity_name})", cluster_name=ClusterName.KEY
        )

    @override
    @cached_property
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet((KeyEntityClusterLabel.get_instance(), KeyEntityLabel.get_instance()))
