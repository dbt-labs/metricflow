# from __future__ import annotations
#
# from enum import Enum
#
# from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
# from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
# from dbt_semantic_interfaces.references import EntityReference
# from dbt_semantic_interfaces.type_enums import TimeGranularity
#
# from metricflow_semantics.experimental.semantic_graph_old.graph_nodes import EntityNode
#
#
# class TimeEntityNodeEnum2(Enum):
#     TIME_ENTITY_NODE = EntityNode(EntityReference("time"))
#
#
# class TimeEntityNodeEnum(Enum):
#     METRIC_TIME_ENTITY_NODE = EntityNode(EntityReference(METRIC_TIME_ELEMENT_NAME))
#     NANOSECOND_NODE = EntityNode(EntityReference(TimeGranularity.NANOSECOND.value))
#     MICROSECOND_NODE = EntityNode(EntityReference(TimeGranularity.MICROSECOND.value))
#     MILLISECOND_NODE = EntityNode(EntityReference(TimeGranularity.MILLISECOND.value))
#     SECOND_NODE = EntityNode(EntityReference(TimeGranularity.SECOND.value))
#     MINUTE_NODE = EntityNode(EntityReference(TimeGranularity.MINUTE.value))
#     HOUR_NODE = EntityNode(EntityReference(TimeGranularity.HOUR.value))
#     DAY_NODE = EntityNode(EntityReference(TimeGranularity.DAY.value))
#     WEEK_NODE = EntityNode(EntityReference(TimeGranularity.WEEK.value))
#     MONTH_NODE = EntityNode(EntityReference(TimeGranularity.MONTH.value))
#     QUARTER_NODE = EntityNode(EntityReference(TimeGranularity.QUARTER.value))
#     YEAR_NODE = EntityNode(EntityReference(TimeGranularity.YEAR.value))
#
#     @staticmethod
#     def get_for_time_grain(time_grain: TimeGranularity) -> EntityNode:
#         if time_grain is TimeGranularity.NANOSECOND:
#             return TimeEntityNodeEnum.NANOSECOND_NODE.value
#         elif time_grain is TimeGranularity.MICROSECOND:
#             return TimeEntityNodeEnum.MICROSECOND_NODE.value
#         elif time_grain is TimeGranularity.MICROSECOND:
#             return TimeEntityNodeEnum.MILLISECOND_NODE.value
#         elif time_grain is TimeGranularity.MILLISECOND:
#             return TimeEntityNodeEnum.MILLISECOND_NODE.value
#         elif time_grain is TimeGranularity.SECOND:
#             return TimeEntityNodeEnum.SECOND_NODE.value
#         elif time_grain is TimeGranularity.MINUTE:
#             return TimeEntityNodeEnum.MINUTE_NODE.value
#         elif time_grain is TimeGranularity.HOUR:
#             return TimeEntityNodeEnum.HOUR_NODE.value
#         elif time_grain is TimeGranularity.DAY:
#             return TimeEntityNodeEnum.DAY_NODE.value
#         elif time_grain is TimeGranularity.WEEK:
#             return TimeEntityNodeEnum.WEEK_NODE.value
#         elif time_grain is TimeGranularity.MONTH:
#             return TimeEntityNodeEnum.MONTH_NODE.value
#         elif time_grain is TimeGranularity.QUARTER:
#             return TimeEntityNodeEnum.QUARTER_NODE.value
#         elif time_grain is TimeGranularity.YEAR:
#             return TimeEntityNodeEnum.YEAR_NODE.value
#         else:
#             assert_values_exhausted(time_grain)
