from __future__ import annotations

from enum import Enum
from typing import List

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
    DOW = "dow"
    DOY = "doy"

    def to_int(self) -> int:
        """Convert to an int so that the size of the granularity can be easily compared."""
        if self is DatePart.DAY:
            return TimeGranularity.DAY.to_int()
        elif self is DatePart.DOW:
            return TimeGranularity.DAY.to_int()
        elif self is DatePart.DOY:
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

    @property
    def compatible_granularities(self) -> List[TimeGranularity]:
        """Granularities that can be queried with this date part."""
        return [granularity for granularity in TimeGranularity if granularity.to_int() >= self.to_int()]
