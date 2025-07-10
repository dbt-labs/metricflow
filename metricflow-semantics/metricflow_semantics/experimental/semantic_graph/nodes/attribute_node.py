from __future__ import annotations

from abc import ABC
from functools import cached_property

from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity
from typing_extensions import override

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
    GroupByAttributeLabel,
    GroupByMetricLabel,
    KeyEntityClusterLabel,
    MeasureLabel,
    TimeClusterLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.sg_constant import ClusterName
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.time.granularity import ExpandedTimeGranularity


@singleton_dataclass(order=False)
class AttributeNode(SemanticGraphNode, ABC):
    attribute_name: str

    @cached_property
    def comparison_key(self) -> ComparisonKey:
        return (self.attribute_name,)

    @property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(node_name=f"{self.__class__.__name__}({self.attribute_name})")

    @override
    def as_dot_node(self, include_graphical_attributes: bool) -> DotNodeAttributeSet:
        dot_node = super(AttributeNode, self).as_dot_node(include_graphical_attributes)
        if include_graphical_attributes:
            dot_node = dot_node.with_attributes(color=DotColor.SALMON_PINK)
        return dot_node

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(SemanticGraphNode, self).labels.union((GroupByAttributeLabel(),))

    @override
    @cached_property
    def attribute_recipe_update(self) -> AttributeRecipeUpdate:
        return AttributeRecipeUpdate(
            add_dunder_name_element=self.attribute_name,
        )


@singleton_dataclass(order=False)
class TimeAttributeNode(AttributeNode):
    linkable_element_property_additions: FrozenOrderedSet[LinkableElementProperty]

    @staticmethod
    def get_instance_for_time_grain(time_grain: TimeGranularity) -> TimeAttributeNode:
        return TimeAttributeNode(
            attribute_name=time_grain.value,
            linkable_element_property_additions=FrozenOrderedSet(),
        )

    @staticmethod
    def get_instance_for_date_part(date_part: DatePart) -> TimeAttributeNode:
        return TimeAttributeNode(
            attribute_name=StructuredLinkableSpecName.date_part_suffix(date_part),
            linkable_element_property_additions=FrozenOrderedSet((LinkableElementProperty.DATE_PART,)),
        )

    @staticmethod
    def get_instance_for_expanded_time_grain(expanded_time_grain: ExpandedTimeGranularity) -> TimeAttributeNode:
        return TimeAttributeNode(
            attribute_name=expanded_time_grain.name,
            linkable_element_property_additions=FrozenOrderedSet((LinkableElementProperty.DERIVED_TIME_GRANULARITY,)),
        )

    @property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"TimeAttribute({self.attribute_name})", cluster_name="time"
        )

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(TimeAttributeNode, self).labels.union((TimeClusterLabel.get_instance(),))


@singleton_dataclass(order=False)
class MeasureNode(AttributeNode):
    model_id: SemanticModelId
    _labels: FrozenOrderedSet[MetricflowGraphLabel]

    @staticmethod
    def get_instance(measure_name: str, model_id: SemanticModelId) -> MeasureNode:
        return MeasureNode(
            attribute_name=measure_name,
            model_id=model_id,
            _labels=FrozenOrderedSet((MeasureLabel(measure_name=None), MeasureLabel(measure_name=measure_name))),
        )

    @property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"Measure({self.attribute_name})", cluster_name=self.model_id.cluster_name
        )

    @override
    def as_dot_node(self, include_graphical_attributes: bool) -> DotNodeAttributeSet:
        dot_node = super(AttributeNode, self).as_dot_node(include_graphical_attributes)
        if include_graphical_attributes:
            dot_node = dot_node.with_attributes(color=DotColor.LIME_GREEN)
        return dot_node

    @override
    @property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return self._labels

    @override
    @cached_property
    def attribute_recipe_update(self) -> AttributeRecipeUpdate:
        # return AttributeComputationUpdate(
        #     derived_from_model_id_additions=(self.model_id,),
        # )
        return AttributeRecipeUpdate(join_model=self.model_id)

    # @override
    # def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
    #     formatter = MetricFlowPrettyFormatter(
    #         format_option=format_context.formatter.format_option.merge(
    #             PrettyFormatOption(include_underscore_prefix_fields=True)
    #         )
    #     )
    #     return formatter.pretty_format_object_by_parts(
    #         class_name=self.__class__.__name__,
    #         field_mapping=dataclasses.asdict(self),
    #     )


@singleton_dataclass(order=False)
class KeyAttributeNode(AttributeNode):
    @staticmethod
    def get_instance(entity_name: str) -> KeyAttributeNode:
        return KeyAttributeNode(attribute_name=entity_name)

    @property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"KeyAttribute({self.attribute_name})",
            cluster_name=ClusterName.KEY,
        )

    @override
    @cached_property
    def attribute_recipe_update(self) -> AttributeRecipeUpdate:
        return AttributeRecipeUpdate(
            add_dunder_name_element=self.attribute_name,
            add_properties=(LinkableElementProperty.ENTITY,),
            set_element_type=LinkableElementType.ENTITY,
        )

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(KeyAttributeNode, self).labels.union((KeyEntityClusterLabel.get_instance(),))


@singleton_dataclass(order=False)
class CategoricalDimensionAttributeNode(AttributeNode):
    @property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"Dimension({self.attribute_name})", cluster_name=ClusterName.DIMENSION
        )

    @override
    @property
    def attribute_recipe_update(self) -> AttributeRecipeUpdate:
        return AttributeRecipeUpdate(
            add_dunder_name_element=self.attribute_name,
            set_element_type=LinkableElementType.DIMENSION,
        )


@singleton_dataclass(order=False)
class GroupByMetricNode(AttributeNode):
    @staticmethod
    def get_instance(metric_name: str) -> GroupByMetricNode:  # noqa: D102
        return GroupByMetricNode(attribute_name=metric_name)

    @property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"GroupByMetric({self.attribute_name})", cluster_name=ClusterName.KEY
        )

    @override
    @cached_property
    def attribute_recipe_update(self) -> AttributeRecipeUpdate:
        return AttributeRecipeUpdate(
            add_dunder_name_element=self.attribute_name,
            # derived_from_model_id_additions=tuple(self.source_semantic_models),
            add_properties=(LinkableElementProperty.METRIC,),
            set_element_type=LinkableElementType.METRIC,
        )

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(GroupByMetricNode, self).labels.union(
            (
                GroupByAttributeLabel.get_instance(),
                GroupByMetricLabel.get_instance(),
                GroupByMetricLabel.get_instance(metric_name=self.attribute_name),
            )
        )
