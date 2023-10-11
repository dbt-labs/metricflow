from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.protocols.query_parameter import InputOrderByParameter
from metricflow.time.date_part import DatePart


@dataclass(frozen=True)
class TimeDimensionParameter:
    """Time dimension requested in a query."""

    name: str
    grain: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None


@dataclass(frozen=True)
class DimensionOrEntityParameter:
    """Group by parameter requested in a query.

    Might represent an entity or a dimension.
    """

    name: str


@dataclass(frozen=True)
class MetricParameter:
    """Metric requested in a query."""

    name: str


@dataclass(frozen=True)
class OrderByParameter:
    """Order by requested in a query."""

    order_by: InputOrderByParameter
    descending: bool = False
