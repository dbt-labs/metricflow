from __future__ import annotations

from enum import Enum

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity


class DatePart(Enum):
    """Date parts able to be extracted from a time dimension.

    TODO: add support for hour, minute, second once those granularities are available
    """

    YEAR = "year"
    QUARTER = "quarter"
    MONTH = "month"
    WEEK = "week"
    DAY = "day"
    DAYOFWEEK = "dayofweek"
    DAYOFYEAR = "dayofyear"

    def to_int(self) -> int:
        """Convert to an int so that the size of the granularity can be easily compared."""
        if self is DatePart.DAY:
            return TimeGranularity.DAY.to_int()
        elif self is DatePart.DAYOFWEEK:
            return TimeGranularity.DAY.to_int()
        elif self is DatePart.DAYOFYEAR:
            return TimeGranularity.DAY.to_int()
        elif self is DatePart.WEEK:
            return TimeGranularity.WEEK.to_int()
        elif self is DatePart.MONTH:
            return TimeGranularity.MONTH.to_int()
        elif self is DatePart.QUARTER:
            return TimeGranularity.QUARTER.to_int()
        elif self is DatePart.YEAR:
            return TimeGranularity.YEAR.to_int()
        else:
            assert_values_exhausted(self)
