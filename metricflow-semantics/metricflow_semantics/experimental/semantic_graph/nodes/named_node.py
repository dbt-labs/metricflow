from __future__ import annotations

from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import (
    MetricTimeDimensionNode,
    TimeBaseNode,
)


class SemanticGraphNodeFactory:
    @staticmethod
    def get_time_base_node(base_grain: TimeGranularity) -> TimeBaseNode:
        return TimeBaseNode(time_grain_name=base_grain.name)

    @staticmethod
    def get_metric_time_base_node(base_grain: TimeGranularity) -> MetricTimeDimensionNode:
        return MetricTimeDimensionNode.get_instance(time_grain_name=base_grain.value)

    @staticmethod
    def get_time_base_node_by_name(time_grain_name: str) -> TimeBaseNode:
        return TimeBaseNode(time_grain_name=time_grain_name)
