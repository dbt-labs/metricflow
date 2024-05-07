from __future__ import annotations

import datetime
from datetime import date
from typing import Optional, Union

import pandas as pd
from dbt_semantic_interfaces.enum_extension import ExtendedEnum, assert_values_exhausted
from dbt_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.time.time_period import TimePeriodAdjuster
from typing_extensions import override


class PandasTimePeriodAdjuster(TimePeriodAdjuster):
    """Implementation of time period adjustments using `pandas`.

    This code was copied with minimal modifications from existing code and will be replaced with the `dateutil`
    implementation.
    """

    @override
    def expand_time_constraint_to_fill_granularity(
        self, time_constraint: TimeRangeConstraint, granularity: TimeGranularity
    ) -> TimeRangeConstraint:
        constraint_start = time_constraint.start_time
        constraint_end = time_constraint.end_time

        start_ts = pd.Timestamp(time_constraint.start_time)
        if not is_period_start(granularity, start_ts):
            constraint_start = adjust_to_start_of_period(granularity, start_ts).to_pydatetime()

        end_ts = pd.Timestamp(time_constraint.end_time)
        if not is_period_end(granularity, end_ts):
            constraint_end = adjust_to_end_of_period(granularity, end_ts).to_pydatetime()

        if constraint_start < TimeRangeConstraint.ALL_TIME_BEGIN():
            constraint_start = TimeRangeConstraint.ALL_TIME_BEGIN()
        if constraint_end > TimeRangeConstraint.ALL_TIME_END():
            constraint_end = TimeRangeConstraint.ALL_TIME_END()

        return TimeRangeConstraint(start_time=constraint_start, end_time=constraint_end)

    @override
    def adjust_to_start_of_period(
        self, time_granularity: TimeGranularity, date_to_adjust: datetime.datetime
    ) -> datetime.datetime:
        return adjust_to_start_of_period(time_granularity, pd.Timestamp(date_to_adjust)).to_pydatetime()

    @override
    def adjust_to_end_of_period(
        self, time_granularity: TimeGranularity, date_to_adjust: datetime.datetime
    ) -> datetime.datetime:
        return adjust_to_end_of_period(time_granularity, pd.Timestamp(date_to_adjust)).to_pydatetime()

    def _adjust_time_constraint_start_by_window(
        self,
        time_range_constraint: TimeRangeConstraint,
        time_granularity: TimeGranularity,
        time_unit_count: int,
    ) -> TimeRangeConstraint:
        """Moves the start of the time constraint back by <time_unit_count> windows.

        if the metric is weekly-active-users (ie window = 1 week) it moves time_constraint.start one week earlier
        """
        start_ts = pd.Timestamp(time_range_constraint.start_time)
        offset = offset_period(time_granularity) * time_unit_count
        adjusted_start = (start_ts - offset).to_pydatetime()
        return TimeRangeConstraint(
            start_time=adjusted_start,
            end_time=time_range_constraint.end_time,
        )

    @override
    def expand_time_constraint_for_cumulative_metric(
        self, time_constraint: TimeRangeConstraint, granularity: Optional[TimeGranularity], count: int
    ) -> TimeRangeConstraint:
        if granularity is not None:
            return self._adjust_time_constraint_start_by_window(time_constraint, granularity, count)

        # if no window is specified we want to accumulate from the beginning of time
        return TimeRangeConstraint(
            start_time=TimeRangeConstraint.ALL_TIME_BEGIN(),
            end_time=time_constraint.end_time,
        )


def is_period_start(time_granularity: TimeGranularity, date: Union[pd.Timestamp, date]) -> bool:  # noqa: D103
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


def is_period_end(time_granularity: TimeGranularity, date: Union[pd.Timestamp, date]) -> bool:  # noqa: D103
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


def period_begin_offset(  # noqa: D103
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


def period_end_offset(  # noqa: D103
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
