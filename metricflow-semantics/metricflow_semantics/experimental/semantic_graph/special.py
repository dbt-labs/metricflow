from __future__ import annotations

from enum import Enum

from dbt_semantic_interfaces.references import EntityReference

from metricflow_semantics.experimental.semantic_graph.references import AttributeReference


class SpecialEntity(Enum):
    METRIC_TIME = EntityReference("metric_time")


class SpecialAttribute(Enum):
    DAY = AttributeReference("day")
    WEEK = AttributeReference("week")
    MONTH = AttributeReference("month")
    QUARTER = AttributeReference("quarter")
    YEAR = AttributeReference("year")
