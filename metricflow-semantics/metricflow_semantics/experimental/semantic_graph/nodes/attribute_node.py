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
    AttributeComputationUpdate,
)
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    DsiEntityKeyAttributeLabel,
    DunderNameElementLabel,
    GroupByAttributeLabel,
    MeasureAttributeLabel,
    MetricAttributeLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphNode,
)
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
    def dunder_name_element_label(self) -> DunderNameElementLabel:
        return DunderNameElementLabel(element_name=self.attribute_name)

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(SemanticGraphNode, self).labels.union((GroupByAttributeLabel(),))

    @override
    @cached_property
    def attribute_computation_update(self) -> AttributeComputationUpdate:
        return AttributeComputationUpdate(
            dundered_name_element_addition=self.attribute_name,
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
            node_name=f"Time({self.attribute_name})", cluster_name="time_attribute"
        )


@singleton_dataclass(order=False)
class MeasureNode(AttributeNode):
    model_id: SemanticModelId
    _labels: FrozenOrderedSet[MetricflowGraphLabel]

    @staticmethod
    def get_instance(measure_name: str, model_id: SemanticModelId) -> MeasureNode:
        return MeasureNode(
            attribute_name=measure_name,
            model_id=model_id,
            _labels=FrozenOrderedSet(
                (MeasureAttributeLabel(measure_name=None), MeasureAttributeLabel(measure_name=measure_name))
            ),
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
    def attribute_computation_update(self) -> AttributeComputationUpdate:
        return AttributeComputationUpdate(
            derived_from_model_id_additions=(self.model_id,),
        )

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
class DsiEntityKeyAttributeNode(AttributeNode):
    @property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"DsiEntityKey({self.attribute_name})",
            cluster_name="other_attribute",
        )

    @override
    @cached_property
    def attribute_computation_update(self) -> AttributeComputationUpdate:
        return AttributeComputationUpdate(
            dundered_name_element_addition=self.attribute_name,
            linkable_element_property_additions=(LinkableElementProperty.ENTITY,),
            element_type_addition=LinkableElementType.ENTITY,
        )

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(DsiEntityKeyAttributeNode, self).labels.union(FrozenOrderedSet((DsiEntityKeyAttributeLabel(),)))


@singleton_dataclass(order=False)
class CategoricalDimensionAttributeNode(AttributeNode):
    @property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"Dimension({self.attribute_name})", cluster_name="other_attribute"
        )

    @override
    @property
    def attribute_computation_update(self) -> AttributeComputationUpdate:
        return AttributeComputationUpdate(
            dundered_name_element_addition=self.attribute_name,
            element_type_addition=LinkableElementType.DIMENSION,
        )


@singleton_dataclass(order=False)
class MetricNode(AttributeNode):
    @property
    @override
    def node_descriptor(self) -> MetricflowGraphNodeDescriptor:
        return MetricflowGraphNodeDescriptor.get_instance(
            node_name=f"Metric({self.attribute_name})", cluster_name="metric_attribute"
        )

    @override
    @cached_property
    def attribute_computation_update(self) -> AttributeComputationUpdate:
        return AttributeComputationUpdate()

    @override
    @cached_property
    def labels(self) -> FrozenOrderedSet[MetricflowGraphLabel]:
        return super(MetricNode, self).labels.union(
            (
                MetricAttributeLabel(metric_name=self.attribute_name),
                MetricAttributeLabel(metric_name=None),
            )
        )
