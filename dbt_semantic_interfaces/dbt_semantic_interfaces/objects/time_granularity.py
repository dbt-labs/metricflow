from __future__ import annotations

from typing import Any

from dbt_semantic_interfaces.enum_extension import ExtendedEnum, assert_values_exhausted


class TimeGranularity(ExtendedEnum):
    """For time dimensions, the smallest possible difference between two time values.

    Needed for calculating adjacency when merging 2 different time ranges.
    """

    # Names are used in parameters to DATE_TRUNC, so don't change them.
    # Values are used to convert user supplied strings to enums.
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"

    def to_int(self) -> int:
        """Convert to an int so that the size of the granularity can be easily compared."""
        if self is TimeGranularity.DAY:
            return 10
        elif self is TimeGranularity.WEEK:
            return 11
        elif self is TimeGranularity.MONTH:
            return 12
        elif self is TimeGranularity.QUARTER:
            return 13
        elif self is TimeGranularity.YEAR:
            return 14
        else:
            assert_values_exhausted(self)

    def is_smaller_than(self, other: TimeGranularity) -> bool:  # noqa: D
        return self.to_int() < other.to_int()

    def is_smaller_than_or_equal(self, other: TimeGranularity) -> bool:  # noqa: D
        return self.to_int() <= other.to_int()

    def __lt__(self, other: Any) -> bool:  # type: ignore [misc] # noqa: D
        if not isinstance(other, TimeGranularity):
            return NotImplemented
        return self.to_int() < other.to_int()

    def __hash__(self) -> int:  # noqa: D
        return self.to_int()

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}.{self.name}"


def string_to_time_granularity(s: str) -> TimeGranularity:  # noqa: D
    values = {item.value: item for item in TimeGranularity}
    return values[s]
