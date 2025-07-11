from __future__ import annotations

import logging
from functools import cached_property
from typing import override

from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
from metricflow_semantics.experimental.mf_graph.formatting.dot_attributes import (
    DotColor,
    DotNodeAttributeSet,
)
from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.mf_graph.node_descriptor import MetricflowGraphNodeDescriptor
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import (
    AttributeRecipeUpdate,
)
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    AggregationLabel,
    DsiEntityLabel,
    GroupByAttributeRootLabel,
    JoinedModelLabel,
    JoinFromLabel,
    JoinViaLabel,
    KeyEntityClusterLabel,
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
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet((DsiEntityLabel.get_instance(),))

    @override
    @cached_property
    def attribute_recipe_update(self) -> AttributeRecipeUpdate:
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
class TimeAggregationNode(SemanticGraphNode):
    min_time_grain: TimeGranularity

    @staticmethod
    def get_instance(min_time_grain: TimeGranularity) -> TimeAggregationNode:
        return TimeAggregationNode(
            min_time_grain=min_time_grain,
        )

    @override
    @cached_property
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"TimeAgg({self.min_time_grain.value})",
            cluster_name=ClusterName.TIME,
        )

    @property
    def comparison_key(self) -> ComparisonKey:
        return (self.min_time_grain.value,)

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(TimeAggregationNode, self).labels.union((AggregationLabel(),))

    @override
    @cached_property
    def attribute_recipe_update(self) -> AttributeRecipeUpdate:
        return AttributeRecipeUpdate(add_min_time_grain=self.min_time_grain)


# @singleton_dataclass(order=False)
# class TimeAggregationNode(SemanticGraphNode):
#     model_id: SemanticModelId
#     aggregation_time_dimension_name: str
#     min_time_grain: TimeGranularity
#
#     @staticmethod
#     def get_instance(
#         model_id: SemanticModelId, aggregation_time_dimension_name: str, min_time_grain: TimeGranularity
#     ) -> TimeAggregationNode:
#         return TimeAggregationNode(
#             model_id=model_id,
#             aggregation_time_dimension_name=aggregation_time_dimension_name,
#             min_time_grain=min_time_grain,
#         )
#
#     @override
#     @cached_property
#     def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
#         return MetricflowGraphNodeDescriptor.get_instance(
#             node_name=f"Agg({self.model_id.model_name}, {self.aggregation_time_dimension_name})",
#             cluster_name=self.model_id.cluster_name,
#         )
#
#     @property
#     def comparison_key(self) -> ComparisonKey:
#         return (self.model_id.comparison_key, self.aggregation_time_dimension_name)
#
#     @override
#     @cached_property
#     def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
#         return super(TimeAggregationNode, self).labels.union((AggregationLabel(),))
#
#     @override
#     @cached_property
#     def attribute_computation_update(self) -> AttributeComputationUpdate:
#         return AttributeComputationUpdate(min_time_grain=self.min_time_grain)


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
    def attribute_recipe_update(self) -> AttributeRecipeUpdate:
        return AttributeRecipeUpdate(
            join_model=self.model_id,
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
    def attribute_recipe_update(self) -> AttributeRecipeUpdate:
        return AttributeRecipeUpdate(
            join_model=self.model_id,
        )


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
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(SemanticGraphNode, self).labels.union((JoinedModelLabel.get_instance(),))

    @override
    @cached_property
    def attribute_recipe_update(self) -> AttributeRecipeUpdate:
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
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(SemanticGraphNode, self).labels.union((LocalModelLabel.get_instance(),))

    @override
    @cached_property
    def attribute_recipe_update(self) -> AttributeRecipeUpdate:
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
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(TimeDimensionNode, self).labels.union((TimeClusterLabel.get_instance(),))

    @override
    @cached_property
    def attribute_recipe_update(self) -> AttributeRecipeUpdate:
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
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(MetricTimeNode, self).labels.union(
            (MetricTimeLabel.get_instance(), TimeClusterLabel.get_instance())
        )

    @override
    @cached_property
    def attribute_recipe_update(self) -> AttributeRecipeUpdate:
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
    def attribute_recipe_update(self) -> AttributeRecipeUpdate:
        return AttributeRecipeUpdate(
            join_model=self.model_id,
        )


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
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(SemanticGraphNode, self).labels.union((TimeClusterLabel.get_instance(),))


@singleton_dataclass(order=False)
class MetricNode(SemanticGraphNode):
    metric_name: str

    @staticmethod
    def get_instance(metric_name: str) -> MetricNode:  # noqa: D102
        return MetricNode(metric_name=metric_name)

    @override
    @property
    def comparison_key(self) -> ComparisonKey:
        return (self.metric_name,)

    @property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"Metric({self.metric_name})", cluster_name="metric"
        )

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(MetricNode, self).labels.union(
            (
                MetricLabel.get_instance(),
                MetricLabel.get_instance(self.metric_name),
            )
        )

    @override
    @property
    def attribute_recipe_update(self) -> AttributeRecipeUpdate:
        return AttributeRecipeUpdate(
            add_dunder_name_element=self.metric_name,
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
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(KeyEntityNode, self).labels.union((KeyEntityClusterLabel.get_instance(),))
