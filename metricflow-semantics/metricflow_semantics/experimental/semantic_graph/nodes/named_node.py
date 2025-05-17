from __future__ import annotations

from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity

from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import AttributeNode, TimeAttributeNode
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import (
    MetricTimeDimensionNode,
    TimeBaseNode,
)
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.time.granularity import ExpandedTimeGranularity


class SemanticGraphNodeFactory:
    @staticmethod
    def get_time_base_node(base_grain: TimeGranularity) -> TimeBaseNode:
        return TimeBaseNode(time_grain_name=base_grain.name)

    @staticmethod
    def get_metric_time_base_node(base_grain: TimeGranularity) -> MetricTimeDimensionNode:
        return MetricTimeDimensionNode.get_instance(time_grain_name=base_grain.name)

    @staticmethod
    def get_time_base_node_by_name(time_grain_name: str) -> TimeBaseNode:
        return TimeBaseNode(time_grain_name=time_grain_name)

    @staticmethod
    def get_time_grain_attribute_node(time_grain: TimeGranularity) -> AttributeNode:
        return TimeAttributeNode(
            attribute_name=time_grain.value,
        )

    @staticmethod
    def get_date_part_attribute_node(date_part: DatePart) -> AttributeNode:
        return TimeAttributeNode(
            attribute_name=StructuredLinkableSpecName.date_part_suffix(date_part),
        )

    @staticmethod
    def get_attribute_node_for_expanded_time_grain(time_grain: ExpandedTimeGranularity) -> AttributeNode:
        return TimeAttributeNode(
            attribute_name=time_grain.name,
        )
