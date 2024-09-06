from __future__ import annotations

from enum import Enum

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.references import DimensionReference, EntityReference
from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow_semantics.experimental.semantic_graph.graph_nodes import DimensionAttributeNode, EntityNode


class TimeEntityNodeEnum(Enum):
    TIME_ENTITY_NODE = EntityNode(EntityReference("time"))
    METRIC_TIME_ENTITY_NODE = EntityNode(EntityReference(METRIC_TIME_ELEMENT_NAME))


class TimeAttributeNodeEnum(Enum):
    NANOSECOND_NODE = DimensionAttributeNode(DimensionReference(TimeGranularity.NANOSECOND.value))
    MICROSECOND_NODE = DimensionAttributeNode(DimensionReference(TimeGranularity.MICROSECOND.value))
    MILLISECOND_NODE = DimensionAttributeNode(DimensionReference(TimeGranularity.MILLISECOND.value))
    SECOND_NODE = DimensionAttributeNode(DimensionReference(TimeGranularity.SECOND.value))
    MINUTE_NODE = DimensionAttributeNode(DimensionReference(TimeGranularity.MINUTE.value))
    HOUR_NODE = DimensionAttributeNode(DimensionReference(TimeGranularity.HOUR.value))
    DAY_NODE = DimensionAttributeNode(DimensionReference(TimeGranularity.DAY.value))
    WEEK_NODE = DimensionAttributeNode(DimensionReference(TimeGranularity.WEEK.value))
    MONTH_NODE = DimensionAttributeNode(DimensionReference(TimeGranularity.MONTH.value))
    QUARTER_NODE = DimensionAttributeNode(DimensionReference(TimeGranularity.QUARTER.value))
    YEAR_NODE = DimensionAttributeNode(DimensionReference(TimeGranularity.YEAR.value))

    @staticmethod
    def get_for_time_grain(time_grain: TimeGranularity) -> DimensionAttributeNode:
        if time_grain is TimeGranularity.NANOSECOND:
            return TimeAttributeNodeEnum.NANOSECOND_NODE.value
        elif time_grain is TimeGranularity.MICROSECOND:
            return TimeAttributeNodeEnum.MICROSECOND_NODE.value
        elif time_grain is TimeGranularity.MICROSECOND:
            return TimeAttributeNodeEnum.MILLISECOND_NODE.value
        elif time_grain is TimeGranularity.MILLISECOND:
            return TimeAttributeNodeEnum.MILLISECOND_NODE.value
        elif time_grain is TimeGranularity.SECOND:
            return TimeAttributeNodeEnum.SECOND_NODE.value
        elif time_grain is TimeGranularity.MINUTE:
            return TimeAttributeNodeEnum.MINUTE_NODE.value
        elif time_grain is TimeGranularity.HOUR:
            return TimeAttributeNodeEnum.HOUR_NODE.value
        elif time_grain is TimeGranularity.DAY:
            return TimeAttributeNodeEnum.DAY_NODE.value
        elif time_grain is TimeGranularity.WEEK:
            return TimeAttributeNodeEnum.WEEK_NODE.value
        elif time_grain is TimeGranularity.MONTH:
            return TimeAttributeNodeEnum.MONTH_NODE.value
        elif time_grain is TimeGranularity.QUARTER:
            return TimeAttributeNodeEnum.QUARTER_NODE.value
        elif time_grain is TimeGranularity.YEAR:
            return TimeAttributeNodeEnum.YEAR_NODE.value
        else:
            assert_values_exhausted(time_grain)
