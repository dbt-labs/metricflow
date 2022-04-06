from __future__ import annotations

from datetime import date
from typing import Union, Any

import pandas as pd

from metricflow.object_utils import assert_values_exhausted, ExtendedEnum


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

    def is_smaller_than(self, other: "TimeGranularity") -> bool:  # noqa: D
        return self.to_int() < other.to_int()

    def is_smaller_than_or_equal(self, other: "TimeGranularity") -> bool:  # noqa: D
        return self.to_int() <= other.to_int()

    @property
    def offset_period(self) -> pd.offsets.DateOffset:
        """Offset object to use for adjusting by one granularity period."""
        # The type checker is throwing errors for some of those arguments, but they are valid.
        if self is TimeGranularity.DAY:
            return pd.offsets.DateOffset(days=1)  # type: ignore
        elif self is TimeGranularity.WEEK:
            return pd.offsets.DateOffset(weeks=1)  # type: ignore
        elif self is TimeGranularity.MONTH:
            return pd.offsets.DateOffset(months=1)
        elif self is TimeGranularity.QUARTER:
            return pd.offsets.DateOffset(months=3)
        elif self is TimeGranularity.YEAR:
            return pd.offsets.DateOffset(years=1)  # type: ignore
        else:
            assert_values_exhausted(self)

    @property
    def format_with_first_or_last(self) -> bool:
        """Indicates that this can only be calculated if query results display the first or last date of the period."""
        return self in [TimeGranularity.MONTH, TimeGranularity.QUARTER, TimeGranularity.YEAR]

    def is_period_start(self, date: Union[pd.Timestamp, date]) -> bool:  # noqa: D
        pd_date = pd.Timestamp(date)

        if self is TimeGranularity.DAY:
            return True
        elif self is TimeGranularity.WEEK:
            return ISOWeekDay.from_pandas_timestamp(pd_date).is_week_start
        elif self is TimeGranularity.MONTH:
            return pd_date.is_month_start
        elif self is TimeGranularity.QUARTER:
            return pd_date.is_quarter_start
        elif self is TimeGranularity.YEAR:
            return pd_date.is_year_start
        else:
            assert_values_exhausted(self)

    def is_period_end(self, date: Union[pd.Timestamp, date]) -> bool:  # noqa: D
        pd_date = pd.Timestamp(date)

        if self is TimeGranularity.DAY:
            return True
        elif self is TimeGranularity.WEEK:
            return ISOWeekDay.from_pandas_timestamp(pd_date).is_week_end
        elif self is TimeGranularity.MONTH:
            return pd_date.is_month_end
        elif self is TimeGranularity.QUARTER:
            return pd_date.is_quarter_end
        elif self is TimeGranularity.YEAR:
            return pd_date.is_year_end
        else:
            assert_values_exhausted(self)

    @property
    def period_begin_offset(  # noqa: D
        self,
    ) -> Union[pd.offsets.MonthBegin, pd.offsets.QuarterBegin, pd.offsets.Week, pd.offsets.YearBegin]:
        if self is TimeGranularity.DAY:
            raise ValueError(f"Can't get period start offset for TimeGranularity.{self.name}.")
        elif self is TimeGranularity.WEEK:
            return pd.offsets.Week(weekday=ISOWeekDay.MONDAY.pandas_value)
        elif self is TimeGranularity.MONTH:
            return pd.offsets.MonthBegin()
        elif self is TimeGranularity.QUARTER:
            return pd.offsets.QuarterBegin(startingMonth=1)
        elif self is TimeGranularity.YEAR:
            return pd.offsets.YearBegin()
        else:
            assert_values_exhausted(self)

    @property
    def period_end_offset(  # noqa: D
        self,
    ) -> Union[pd.offsets.MonthEnd, pd.offsets.QuarterEnd, pd.offsets.Week, pd.offsets.YearEnd]:
        if self is TimeGranularity.DAY:
            raise ValueError(f"Can't get period end offset for TimeGranularity.{self.name}.")
        elif self == TimeGranularity.WEEK:
            return pd.offsets.Week(weekday=ISOWeekDay.SUNDAY.pandas_value)
        elif self is TimeGranularity.MONTH:
            return pd.offsets.MonthEnd()
        elif self is TimeGranularity.QUARTER:
            return pd.offsets.QuarterEnd(startingMonth=3)
        elif self is TimeGranularity.YEAR:
            return pd.offsets.YearEnd()
        else:
            assert_values_exhausted(self)

    def adjust_to_start_of_period(self, date_to_adjust: pd.Timestamp, rollback: bool = True) -> pd.Timestamp:
        """Adjust to start of period if not at start already."""
        if rollback:
            return self.period_begin_offset.rollback(date_to_adjust)
        else:
            return self.period_begin_offset.rollforward(date_to_adjust)

    def adjust_to_end_of_period(self, date_to_adjust: pd.Timestamp, rollforward: bool = True) -> pd.Timestamp:
        """Adjust to end of period if not at end already."""
        if rollforward:
            return self.period_end_offset.rollforward(date_to_adjust)
        else:
            return self.period_end_offset.rollback(date_to_adjust)

    def match_start_or_end_of_period(self, date_to_match: pd.Timestamp, date_to_adjust: pd.Timestamp) -> pd.Timestamp:
        """Adjust date_to_adjust to be start or end of period based on if date_to_match is at start or end of period."""
        if self.is_period_start(date_to_match):
            return self.adjust_to_start_of_period(date_to_adjust)
        elif self.is_period_end(date_to_match):
            return self.adjust_to_end_of_period(date_to_adjust)
        else:
            raise ValueError(
                f"Expected `date_to_match` to fall at the start or end of the granularity period. Got '{date_to_match}' for granularity {self}."
            )

    def __lt__(self, other: Any) -> bool:  # type: ignore [misc] # noqa: D
        if not isinstance(other, TimeGranularity):
            return NotImplemented
        return self.to_int() < other.to_int()

    def __hash__(self) -> int:  # noqa: D
        return self.to_int()

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}.{self.name}"


class ISOWeekDay(ExtendedEnum):
    """Day of week values per ISO standard"""

    MONDAY = 1
    TUESDAY = 2
    WEDNESDAY = 3
    THURSDAY = 4
    FRIDAY = 5
    SATURDAY = 6
    SUNDAY = 7

    @staticmethod
    def from_pandas_timestamp(timestamp: pd.Timestamp) -> ISOWeekDay:
        """Factory for streamlining conversion from a Pandas Timestamp to an ISOWeekDay"""
        return ISOWeekDay(timestamp.isoweekday())

    @property
    def is_week_start(self) -> bool:
        """Return comparison of instance value against ISO standard start of week (Monday)"""
        return self is ISOWeekDay.MONDAY

    @property
    def is_week_end(self) -> bool:
        """Return comparison of instance value against ISO standard end of week (Sunday)"""
        return self is ISOWeekDay.SUNDAY

    @property
    def pandas_value(self) -> int:
        """Returns the pandas int value representation of the ISOWeekDay"""
        return self.value - 1


def string_to_time_granularity(s: str) -> TimeGranularity:  # noqa: D
    values = {item.value: item for item in TimeGranularity}
    return values[s]
