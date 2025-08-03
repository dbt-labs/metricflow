from __future__ import annotations

from abc import ABC
from functools import cached_property

from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity
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
from metricflow_semantics.experimental.semantic_graph.nodes.node_labels import (
    GroupByAttributeLabel,
    KeyAttributeLabel,
    TimeClusterLabel,
)
from metricflow_semantics.experimental.semantic_graph.sg_constant import ClusterNameFactory
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import SemanticGraphNode
from metricflow_semantics.experimental.singleton import Singleton
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.time.granularity import ExpandedTimeGranularity


@fast_frozen_dataclass(order=False)
class AttributeNode(SemanticGraphNode, ABC):
    """ABC for attribute nodes. See `SemanticGraph` for additional context on usage."""

    attribute_name: str

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return (self.attribute_name,)

    @cached_property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor(
            node_name=f"{self.__class__.__name__}({self.attribute_name})",
            cluster_name=None,
        )

    @override
    def as_dot_node(self, include_graphical_attributes: bool) -> DotNodeAttributeSet:
        dot_node = super(AttributeNode, self).as_dot_node(include_graphical_attributes)
        if include_graphical_attributes:
            dot_node = dot_node.with_attributes(color=DotColor.SALMON_PINK)
        return dot_node

    @cached_property
    @override
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return FrozenOrderedSet((GroupByAttributeLabel.get_instance(),))

    @cached_property
    @override
    def recipe_step_to_append(self) -> AttributeRecipeStep:
        return AttributeRecipeStep(
            add_dunder_name_element=self.attribute_name,
        )


@fast_frozen_dataclass(order=False)
class TimeAttributeNode(AttributeNode, Singleton):
    """An attribute node that corresponds to the different time grains available for querying time dimensions.

    e.g. the graph would contain instances for `day`, `dow`, `month`...
    """

    element_property_additions: FrozenOrderedSet[LinkableElementProperty]

    @classmethod
    def get_instance_for_time_grain(cls, time_grain: TimeGranularity) -> TimeAttributeNode:  # noqa: D102
        return cls._get_instance(
            attribute_name=time_grain.value,
            element_property_additions=FrozenOrderedSet(),
        )

    @classmethod
    def get_instance_for_date_part(cls, date_part: DatePart) -> TimeAttributeNode:  # noqa: D102
        return cls._get_instance(
            attribute_name=StructuredLinkableSpecName.date_part_suffix(date_part),
            element_property_additions=FrozenOrderedSet((LinkableElementProperty.DATE_PART,)),
        )

    @classmethod
    def get_instance_for_expanded_time_grain(  # noqa: D102
        cls,
        expanded_time_grain: ExpandedTimeGranularity,
    ) -> TimeAttributeNode:
        return cls._get_instance(
            attribute_name=expanded_time_grain.name,
            element_property_additions=FrozenOrderedSet((LinkableElementProperty.DERIVED_TIME_GRANULARITY,)),
        )

    @cached_property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor(
            node_name=f"TimeAttribute({self.attribute_name})",
            cluster_name=ClusterNameFactory.TIME,
        )

    @cached_property
    @override
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return super(TimeAttributeNode, self).labels.union((TimeClusterLabel.get_instance(),))


@fast_frozen_dataclass(order=False)
class KeyAttributeNode(AttributeNode, Singleton):
    """Represents the attribute corresponding to the value of a configured entity.

    In the current MF interface, a "query for an entity" means a query for the column values associated with the
    configured entity in a semantic model. To avoid confusion between entities in the semantic graph and entities
    configured in semantic models, "query for an entity" is phrased as "query for an entity key" in the semantic graph
    module.

    Consider using the phrase `entity value` instead.
    """

    @classmethod
    def get_instance(cls, entity_name: str) -> KeyAttributeNode:  # noqa: D102
        return cls._get_instance(attribute_name=entity_name)

    @property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor(
            node_name=f"KeyAttribute({self.attribute_name})",
            cluster_name=ClusterNameFactory.KEY,
        )

    @cached_property
    @override
    def recipe_step_to_append(self) -> AttributeRecipeStep:
        return AttributeRecipeStep(
            add_dunder_name_element=self.attribute_name,
            add_properties=(LinkableElementProperty.ENTITY,),
            set_element_type=LinkableElementType.ENTITY,
        )

    @cached_property
    @override
    def labels(self) -> OrderedSet[MetricflowGraphLabel]:
        return super(KeyAttributeNode, self).labels.union((KeyAttributeLabel.get_instance(),))


@fast_frozen_dataclass(order=False)
class CategoricalDimensionAttributeNode(AttributeNode, Singleton):
    """Represents a dimension in a semantic model with `DimensionType.CATEGORICAL`."""

    @classmethod
    def get_instance(cls, dimension_name: str) -> CategoricalDimensionAttributeNode:  # noqa: D102
        return cls._get_instance(attribute_name=dimension_name)

    @property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor(
            node_name=f"Dimension({self.attribute_name})", cluster_name=ClusterNameFactory.DIMENSION
        )

    @property
    @override
    def recipe_step_to_append(self) -> AttributeRecipeStep:
        return AttributeRecipeStep(
            add_dunder_name_element=self.attribute_name,
            set_element_type=LinkableElementType.DIMENSION,
        )
