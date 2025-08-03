from __future__ import annotations

import logging
from abc import ABC
from functools import cached_property

from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from typing_extensions import override

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
from metricflow_semantics.experimental.mf_graph.formatting.dot_attributes import (
    DotColor,
    DotNodeAttributeSet,
)
from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.mf_graph.node_descriptor import MetricflowGraphNodeDescriptor
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_recipe_step import (
    AttributeRecipeStep,
)
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.node_labels import (
    BaseMetricLabel,
    ConfiguredEntityLabel,
    DerivedMetricLabel,
    JoinedModelLabel,
    LocalModelLabel,
    MeasureLabel,
    MetricLabel,
    MetricTimeLabel,
    TimeClusterLabel,
    TimeDimensionLabel,
)
from metricflow_semantics.experimental.semantic_graph.sg_constant import ClusterNameFactory
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import SemanticGraphNode
from metricflow_semantics.experimental.singleton import Singleton
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType

logger = logging.getLogger(__name__)


@fast_frozen_dataclass(order=False)
class ConfiguredEntityNode(SemanticGraphNode, Singleton):
    """Represents an `entity` element as configured in a semantic model / manifest.

    This node is named "configured" to avoid confusion between entities in the semantic manifest and entities in the
    semantic graph.
    """

    entity_name: str
    model_id: SemanticModelId

    @classmethod
    def get_instance(cls, entity_name: str, model_id: SemanticModelId) -> ConfiguredEntityNode:  # noqa: D102
        return cls._get_instance(entity_name=entity_name, model_id=model_id)

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return (self.entity_name, self.model_id)

    @override
    def as_dot_node(self, include_graphical_attributes: bool) -> DotNodeAttributeSet:
        dot_node = super(ConfiguredEntityNode, self).as_dot_node(include_graphical_attributes)
        if include_graphical_attributes:
            dot_node = dot_node.with_attributes(color=DotColor.CORNFLOWER_BLUE)
        return dot_node

    @cached_property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor(
            node_name=f"{self.model_id}.{self.entity_name}",
            cluster_name=ClusterNameFactory.CONFIGURED_ENTITY,
        )

    @cached_property
    @override
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet((ConfiguredEntityLabel.get_instance(),))

    @cached_property
    @override
    def recipe_step_to_append(self) -> AttributeRecipeStep:
        return AttributeRecipeStep(
            add_entity_link=self.entity_name,
            add_dunder_name_element=self.entity_name,
        )


@fast_frozen_dataclass(order=False)
class JoinedModelNode(SemanticGraphNode, Singleton):
    """An entity that represents the attributes accessible from a semantic model when the name includes an entity link.

    In the query interface, the description for a group-by item (e.g. with the dunder name, `listing__country_latest`
    `listing` is the entity link).

    An entity link often means a join between semantic models, but not always. For example, dimensions
    can be queried with the associated primary entity name as the entity link even when the query does not require
    joining semantic models.

    To capture this behavior, semantic models are represented with 2 seperate nodes.
    """

    model_id: SemanticModelId

    @classmethod
    def get_instance(cls, model_id: SemanticModelId) -> JoinedModelNode:  # noqa: D102
        # return JoinedModelNode(model_id=model_id)
        return cls._get_instance(model_id=model_id)

    @cached_property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor(
            node_name=f"JoinedModel({self.model_id})",
            cluster_name=ClusterNameFactory.get_name_for_model(self.model_id),
        )

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return self.model_id.comparison_key

    @cached_property
    @override
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet((JoinedModelLabel.get_instance(),))

    @override
    def as_dot_node(self, include_graphical_attributes: bool) -> DotNodeAttributeSet:
        dot_node = super(JoinedModelNode, self).as_dot_node(include_graphical_attributes)
        if include_graphical_attributes:
            dot_node = dot_node.with_attributes(edge_node_priority=2)
        return dot_node


@fast_frozen_dataclass(order=False)
class LocalModelNode(SemanticGraphNode, Singleton):
    """An entity that represents the attributes accessible from a semantic model without entity links.

    Also see `JoinedModelNode`.
    """

    model_id: SemanticModelId

    @classmethod
    def get_instance(cls, model_id: SemanticModelId) -> LocalModelNode:  # noqa: D102
        # return LocalModelNode(model_id=model_id)
        return cls._get_instance(model_id=model_id)

    @cached_property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor(
            node_name=f"LocalModel({self.model_id})",
            cluster_name=ClusterNameFactory.get_name_for_model(self.model_id),
        )

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return self.model_id.comparison_key

    @cached_property
    @override
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet((LocalModelLabel.get_instance(),))

    @cached_property
    @override
    def recipe_step_to_append(self) -> AttributeRecipeStep:
        return AttributeRecipeStep(add_model_join=self.model_id)

    @override
    def as_dot_node(self, include_graphical_attributes: bool) -> DotNodeAttributeSet:
        dot_node = super(LocalModelNode, self).as_dot_node(include_graphical_attributes)
        if include_graphical_attributes:
            dot_node = dot_node.with_attributes(edge_node_priority=1)
        return dot_node


@fast_frozen_dataclass(order=False)
class TimeDimensionNode(SemanticGraphNode, Singleton):
    """An entity representing a time dimension configured in a semantic model.

    In the semantic graph, time dimensions are represented as entities that are related to the time entity. The time
    entity has edges to the different queryable grain-related attributes (e.g. `day`, `month`).
    """

    dimension_name: str

    @classmethod
    def get_instance(cls, dimension_name: str) -> TimeDimensionNode:  # noqa: D102
        return cls._get_instance(dimension_name=dimension_name)

    @cached_property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor(
            node_name=f"TimeDimension({self.dimension_name})",
            cluster_name=ClusterNameFactory.TIME_DIMENSION,
        )

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return (self.dimension_name,)

    @cached_property
    @override
    def recipe_step_to_append(self) -> AttributeRecipeStep:
        return AttributeRecipeStep(
            add_dunder_name_element=self.dimension_name,
            set_element_type=LinkableElementType.TIME_DIMENSION,
        )

    @override
    def as_dot_node(self, include_graphical_attributes: bool) -> DotNodeAttributeSet:
        dot_node = super(TimeDimensionNode, self).as_dot_node(include_graphical_attributes)
        if include_graphical_attributes:
            dot_node = dot_node.with_attributes(edge_node_priority=1)
        return dot_node


@fast_frozen_dataclass(order=False)
class MetricTimeNode(SemanticGraphNode, Singleton):
    """An entity that represents `metric_time`."""

    @classmethod
    def get_instance(cls) -> MetricTimeNode:  # noqa: D102
        return cls._get_instance()

    @cached_property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor(
            node_name="MetricTime",
            cluster_name=ClusterNameFactory.TIME_DIMENSION,
        )

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return ()

    @cached_property
    @override
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet((MetricTimeLabel.get_instance(), TimeDimensionLabel.get_instance()))

    @cached_property
    @override
    def recipe_step_to_append(self) -> AttributeRecipeStep:
        return AttributeRecipeStep(
            add_dunder_name_element=METRIC_TIME_ELEMENT_NAME,
            add_properties=(LinkableElementProperty.METRIC_TIME,),
            set_element_type=LinkableElementType.TIME_DIMENSION,
        )

    @override
    def as_dot_node(self, include_graphical_attributes: bool) -> DotNodeAttributeSet:
        dot_node = super(MetricTimeNode, self).as_dot_node(include_graphical_attributes)
        if include_graphical_attributes:
            dot_node = dot_node.with_attributes(edge_node_priority=1)
        return dot_node


@fast_frozen_dataclass(order=False)
class TimeNode(SemanticGraphNode, Singleton):
    """An entity representing time.

    Other entities related to time (time dimensions, metric time) have an edge to this node.
    """

    @classmethod
    def get_instance(cls) -> TimeNode:  # noqa: D102
        return cls._get_instance()

    @cached_property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor(
            node_name="TimeEntity",
            cluster_name=ClusterNameFactory.TIME,
        )

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return ()

    @cached_property
    @override
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet((TimeClusterLabel.get_instance(),))


@fast_frozen_dataclass(order=False)
class MetricNode(SemanticGraphNode, ABC):
    """ABC for nodes that represent a metric."""

    metric_name: str

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return (self.metric_name,)

    @cached_property
    @override
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet((MetricLabel.get_instance(), MetricLabel.get_instance(self.metric_name)))

    @cached_property
    @override
    def recipe_step_to_append(self) -> AttributeRecipeStep:
        return AttributeRecipeStep(
            add_dunder_name_element=self.metric_name,
        )


@fast_frozen_dataclass(order=False)
class BaseMetricNode(MetricNode, Singleton):
    """Represents a metric without successors to other metric nodes.

    All non-derived metrics are represented by this node.
    """

    @classmethod
    def get_instance(cls, metric_name: str) -> BaseMetricNode:  # noqa: D102
        return cls._get_instance(metric_name=metric_name)

    @cached_property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor(
            node_name=f"BaseMetric({self.metric_name})", cluster_name=ClusterNameFactory.METRIC
        )

    @cached_property
    @override
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return super(BaseMetricNode, self).labels.union((BaseMetricLabel.get_instance(),))


@fast_frozen_dataclass(order=False)
class DerivedMetricNode(MetricNode, Singleton):
    """Represents derived metrics."""

    @classmethod
    def get_instance(cls, metric_name: str) -> DerivedMetricNode:  # noqa: D102
        return cls._get_instance(metric_name=metric_name)

    @cached_property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor(
            node_name=f"DerivedMetric({self.metric_name})", cluster_name=ClusterNameFactory.METRIC
        )

    @cached_property
    @override
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return super(DerivedMetricNode, self).labels.union((DerivedMetricLabel.get_instance(),))


@fast_frozen_dataclass(order=False)
class MeasureNode(SemanticGraphNode, Singleton):
    """Represents measures.

    * Currently modeled as an entity since it's not a leaf node.
    * The successors of this node include `LocalModelNode`, `JoinedModelNode`, and `MetricTimeNode`.
    """

    measure_name: str
    source_model_id: SemanticModelId

    @classmethod
    def get_instance(cls, measure_name: str, source_model_id: SemanticModelId) -> MeasureNode:  # noqa: D102
        return cls._get_instance(measure_name=measure_name, source_model_id=source_model_id)

    @cached_property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor(
            node_name=f"Measure({self.measure_name})",
            cluster_name=ClusterNameFactory.get_name_for_model(self.source_model_id),
        )

    @override
    def as_dot_node(self, include_graphical_attributes: bool) -> DotNodeAttributeSet:
        dot_node = super(SemanticGraphNode, self).as_dot_node(include_graphical_attributes)
        if include_graphical_attributes:
            dot_node = dot_node.with_attributes(color=DotColor.LIME_GREEN, edge_node_priority=2)
        return dot_node

    @cached_property
    @override
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet(
            (MeasureLabel.get_instance(measure_name=None), MeasureLabel.get_instance(measure_name=self.measure_name))
        )

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return (self.measure_name,)
