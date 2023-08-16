from __future__ import annotations

from datetime import date
from typing import Union

import pandas as pd
from dbt_semantic_interfaces.enum_extension import ExtendedEnum, assert_values_exhausted
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity


def offset_period(time_granularity: TimeGranularity) -> pd.offsets.DateOffset:
    """Offset object to use for adjusting by one granularity period."""
    # The type checker is throwing errors for some of those arguments, but they are valid.
    if time_granularity is TimeGranularity.DAY:
        return pd.offsets.DateOffset(days=1)  # type: ignore
    elif time_granularity is TimeGranularity.WEEK:
        return pd.offsets.DateOffset(weeks=1)  # type: ignore
    elif time_granularity is TimeGranularity.MONTH:
        return pd.offsets.DateOffset(months=1)
    elif time_granularity is TimeGranularity.QUARTER:
        return pd.offsets.DateOffset(months=3)
    elif time_granularity is TimeGranularity.YEAR:
        return pd.offsets.DateOffset(years=1)  # type: ignore
    else:
        assert_values_exhausted(time_granularity)


def format_with_first_or_last(time_granularity: TimeGranularity) -> bool:
    """Indicates that this can only be calculated if query results display the first or last date of the period."""
    return time_granularity in [TimeGranularity.MONTH, TimeGranularity.QUARTER, TimeGranularity.YEAR]


def is_period_start(time_granularity: TimeGranularity, date: Union[pd.Timestamp, date]) -> bool:  # noqa: D
    pd_date = pd.Timestamp(date)

    if time_granularity is TimeGranularity.DAY:
        return True
    elif time_granularity is TimeGranularity.WEEK:
        return ISOWeekDay.from_pandas_timestamp(pd_date).is_week_start
    elif time_granularity is TimeGranularity.MONTH:
        return pd_date.is_month_start
    elif time_granularity is TimeGranularity.QUARTER:
        return pd_date.is_quarter_start
    elif time_granularity is TimeGranularity.YEAR:
        return pd_date.is_year_start
    else:
        assert_values_exhausted(time_granularity)


def is_period_end(time_granularity: TimeGranularity, date: Union[pd.Timestamp, date]) -> bool:  # noqa: D
    pd_date = pd.Timestamp(date)

    if time_granularity is TimeGranularity.DAY:
        return True
    elif time_granularity is TimeGranularity.WEEK:
        return ISOWeekDay.from_pandas_timestamp(pd_date).is_week_end
    elif time_granularity is TimeGranularity.MONTH:
        return pd_date.is_month_end
    elif time_granularity is TimeGranularity.QUARTER:
        return pd_date.is_quarter_end
    elif time_granularity is TimeGranularity.YEAR:
        return pd_date.is_year_end
    else:
        assert_values_exhausted(time_granularity)


def period_begin_offset(  # noqa: D
    time_granularity: TimeGranularity,
) -> Union[pd.offsets.MonthBegin, pd.offsets.QuarterBegin, pd.offsets.Week, pd.offsets.YearBegin]:
    if time_granularity is TimeGranularity.DAY:
        raise ValueError(f"Can't get period start offset for TimeGranularity.{time_granularity.name}.")
    elif time_granularity is TimeGranularity.WEEK:
        return pd.offsets.Week(weekday=ISOWeekDay.MONDAY.pandas_value)
    elif time_granularity is TimeGranularity.MONTH:
        return pd.offsets.MonthBegin()
    elif time_granularity is TimeGranularity.QUARTER:
        return pd.offsets.QuarterBegin(startingMonth=1)
    elif time_granularity is TimeGranularity.YEAR:
        return pd.offsets.YearBegin()
    else:
        assert_values_exhausted(time_granularity)


def period_end_offset(  # noqa: D
    time_granularity: TimeGranularity,
) -> Union[pd.offsets.MonthEnd, pd.offsets.QuarterEnd, pd.offsets.Week, pd.offsets.YearEnd]:
    if time_granularity is TimeGranularity.DAY:
        raise ValueError(f"Can't get period end offset for TimeGranularity.{time_granularity.name}.")
    elif time_granularity == TimeGranularity.WEEK:
        return pd.offsets.Week(weekday=ISOWeekDay.SUNDAY.pandas_value)
    elif time_granularity is TimeGranularity.MONTH:
        return pd.offsets.MonthEnd()
    elif time_granularity is TimeGranularity.QUARTER:
        return pd.offsets.QuarterEnd(startingMonth=3)
    elif time_granularity is TimeGranularity.YEAR:
        return pd.offsets.YearEnd()
    else:
        assert_values_exhausted(time_granularity)


def adjust_to_start_of_period(
    time_granularity: TimeGranularity, date_to_adjust: pd.Timestamp, rollback: bool = True
) -> pd.Timestamp:
    """Adjust to start of period if not at start already."""
    if rollback:
        return period_begin_offset(time_granularity).rollback(date_to_adjust)
    else:
        return period_begin_offset(time_granularity).rollforward(date_to_adjust)


def adjust_to_end_of_period(
    time_granularity: TimeGranularity, date_to_adjust: pd.Timestamp, rollforward: bool = True
) -> pd.Timestamp:
    """Adjust to end of period if not at end already."""
    if rollforward:
        return period_end_offset(time_granularity).rollforward(date_to_adjust)
    else:
        return period_end_offset(time_granularity).rollback(date_to_adjust)


def match_start_or_end_of_period(
    time_granularity: TimeGranularity, date_to_match: pd.Timestamp, date_to_adjust: pd.Timestamp
) -> pd.Timestamp:
    """Adjust date_to_adjust to be start or end of period based on if date_to_match is at start or end of period."""
    if is_period_start(time_granularity, date_to_match):
        return adjust_to_start_of_period(time_granularity, date_to_adjust)
    elif is_period_end(time_granularity, date_to_match):
        return adjust_to_end_of_period(time_granularity, date_to_adjust)
    else:
        raise ValueError(
            f"Expected `date_to_match` to fall at the start or end of the granularity period. Got '{date_to_match}' for granularity {time_granularity}."
        )


class ISOWeekDay(ExtendedEnum):
    """Day of week values per ISO standard."""

    MONDAY = 1
    TUESDAY = 2
    WEDNESDAY = 3
    THURSDAY = 4
    FRIDAY = 5
    SATURDAY = 6
    SUNDAY = 7

    @staticmethod
    def from_pandas_timestamp(timestamp: pd.Timestamp) -> ISOWeekDay:
        """Factory for streamlining conversion from a Pandas Timestamp to an ISOWeekDay."""
        return ISOWeekDay(timestamp.isoweekday())

    @property
    def is_week_start(self) -> bool:
        """Return comparison of instance value against ISO standard start of week (Monday)."""
        return self is ISOWeekDay.MONDAY

    @property
    def is_week_end(self) -> bool:
        """Return comparison of instance value against ISO standard end of week (Sunday)."""
        return self is ISOWeekDay.SUNDAY

    @property
    def pandas_value(self) -> int:
        """Returns the pandas int value representation of the ISOWeekDay."""
        return self.value - 1


def string_to_time_granularity(s: str) -> TimeGranularity:  # noqa: D
    values = {item.value: item for item in TimeGranularity}
    return values[s]
