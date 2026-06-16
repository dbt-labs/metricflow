from __future__ import annotations

from typing import List

from metricflow_semantic_interfaces.enum_extension import ExtendedEnum, assert_values_exhausted
from metricflow_semantic_interfaces.type_enums.time_granularity import TimeGranularity


class DatePart(ExtendedEnum):
    """Date parts able to be extracted from a time dimension.

    Note this does not support WEEK (aka WEEKOFYEAR), because week numbering is very strange.
    The ISO spec calls for weeks to start on Monday. Fair enough. It also calls for years to
    start on Monday, but only about 1 out of every 7 do. In order to ensure years start on
    Monday, the ISO decided that the first day of any given year is the Monday of the week
    containing the first Thursday of that year. Consequently, the ISO standard produces
    weeks numbered 1-53, but any days belonging to the preceding calendar year but in the
    first week of the new year are part of the new ISO year. This is not really what people
    expect.

    But there's more - different SQL engines also have different implementations of week of year.
    When not using ISO, you get either 0-53, 1-54, or 1-53 with different ways of deciding
    how to count the first few days in any given year. As such, we just don't support this.

    When the time comes, we can support week using whatever standard makes the most sense for
    our usage context, but as it is not clear what that standard looks like we simply don't
    support date_part = week for now.

    TODO: add support for hour, minute, second once those granularities are available
    """

    YEAR = "year"
    QUARTER = "quarter"
    MONTH = "month"
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
        return [granularity for granularity in TimeGranularity if granularity.to_int() <= self.to_int()]
