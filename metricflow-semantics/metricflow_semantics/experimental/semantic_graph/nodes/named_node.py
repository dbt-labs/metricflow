from __future__ import annotations

from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.entity_id import VirtualEntityId
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import EntityNode


class NamedNode:
    @staticmethod
    def get_time_base_node(time_grain: TimeGranularity) -> EntityNode:
        return NamedNode.get_time_base_node_by_name(time_grain.value)

    @staticmethod
    def get_time_base_node_by_name(time_grain_name: str) -> EntityNode:
        return EntityNode.get_instance(
            entity_id=VirtualEntityId.get_instance(time_grain_name + "_base"),
            entity_link_name=time_grain_name,
            properties=FrozenOrderedSet(),
        )
