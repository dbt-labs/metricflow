from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Union

from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.protocols.query_parameter import (
    GroupByQueryParameter,
    MetricQueryParameter,
    TimeDimensionQueryParameter,
)
from metricflow.time.date_part import DatePart


@dataclass(frozen=True)
class TimeDimensionParameter:
    """Time dimension requested in a query."""

    name: str
    grain: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None

    def __post_init__(self) -> None:  # noqa: D
        parsed_name = StructuredLinkableSpecName.from_name(self.name)
        if parsed_name.time_granularity:
            raise ValueError("Must use object syntax for `grain` parameter if `date_part` is requested.")


@dataclass(frozen=True)
class GroupByParameter:
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

    order_by: Union[MetricQueryParameter, GroupByQueryParameter, TimeDimensionQueryParameter]
    descending: bool = False


# Do we want one generic type for QueryParameter which includes grain & date_part?
# The main question: do we need to know what type we're passing into MF? Or are we ok with MF just figuring it out?
