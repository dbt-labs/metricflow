from __future__ import annotations

import datetime
from typing import Optional

import dateutil.relativedelta
from dateutil.relativedelta import relativedelta
from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums import TimeGranularity
from typing_extensions import override

from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.time.time_period import TimePeriodAdjuster


class DateutilTimePeriodAdjuster(TimePeriodAdjuster):
    """Implementation of time period adjustments using `dateutil`.

    * `relativedelta` will not change weekday if already at the given weekday, even with a Nth parameter.
    * `relativedelta` will automatically handle day values that exceed the number of days in months with < 31 days.
    """

    def _relative_delta_for_window(self, time_granularity: TimeGranularity, count: int) -> relativedelta:
        """Relative-delta to cover time windows specified at different grains."""
        if (
            time_granularity is TimeGranularity.NANOSECOND
            or time_granularity is TimeGranularity.MICROSECOND
            or time_granularity is TimeGranularity.MILLISECOND
        ):
            raise ValueError("Time constraints only support SECOND or larger time granularities.")
        elif time_granularity is TimeGranularity.SECOND:
            return relativedelta(seconds=count)
        elif time_granularity is TimeGranularity.MINUTE:
            return relativedelta(minutes=count)
        elif time_granularity is TimeGranularity.HOUR:
            return relativedelta(hours=count)
        elif time_granularity is TimeGranularity.DAY:
            return relativedelta(days=count)
        elif time_granularity is TimeGranularity.WEEK:
            return relativedelta(weeks=count)
        elif time_granularity is TimeGranularity.MONTH:
            return relativedelta(months=count)
        elif time_granularity is TimeGranularity.QUARTER:
            return relativedelta(months=count * 3)
        elif time_granularity is TimeGranularity.YEAR:
            return relativedelta(years=count)
        else:
            assert_values_exhausted(time_granularity)

    @override
    def expand_time_constraint_to_fill_granularity(
        self, time_constraint: TimeRangeConstraint, granularity: TimeGranularity
    ) -> TimeRangeConstraint:
        adjusted_start = self.adjust_to_start_of_period(granularity, time_constraint.start_time)
        adjusted_end = self.adjust_to_end_of_period(granularity, time_constraint.end_time)

        if adjusted_start < TimeRangeConstraint.ALL_TIME_BEGIN():
            adjusted_start = TimeRangeConstraint.ALL_TIME_BEGIN()
        if adjusted_end > TimeRangeConstraint.ALL_TIME_END():
            adjusted_end = TimeRangeConstraint.ALL_TIME_END()

        return TimeRangeConstraint(start_time=adjusted_start, end_time=adjusted_end)

    @override
    def adjust_to_start_of_period(
        self, time_granularity: TimeGranularity, date_to_adjust: datetime.datetime
    ) -> datetime.datetime:
        """Takes a date and adjusts it to the start of the given granularity period.

        Used to adjust time constraints when the time dimension they will be applied to has a larger granularity
        than the time constraint requested.

        Args:
            time_granularity: The granularity of the time dimension that will be constrained.
            date_to_adjust: The time constraint start date.
        """
        # Strip time data that is irrelevant to avoid rendering an unnecessarily complex time constraint.
        date_to_adjust = date_to_adjust.replace(microsecond=0)
        if time_granularity.to_int() >= TimeGranularity.DAY.to_int():
            date_to_adjust = date_to_adjust.replace(hour=0, minute=0, second=0)

        if (
            time_granularity is TimeGranularity.NANOSECOND
            or time_granularity is TimeGranularity.MICROSECOND
            or time_granularity is TimeGranularity.MILLISECOND
        ):
            raise ValueError("Time constraints only support SECOND and larger time granularities.")
        elif time_granularity is TimeGranularity.SECOND:
            return date_to_adjust
        elif time_granularity is TimeGranularity.MINUTE:
            return date_to_adjust + relativedelta(second=0)
        elif time_granularity is TimeGranularity.HOUR:
            return date_to_adjust + relativedelta(minute=0, second=0)
        elif time_granularity is TimeGranularity.DAY:
            return date_to_adjust
        elif time_granularity is TimeGranularity.WEEK:
            return date_to_adjust + relativedelta(weekday=dateutil.relativedelta.MO(-1))
        elif time_granularity is TimeGranularity.MONTH:
            return date_to_adjust + relativedelta(day=1)
        elif time_granularity is TimeGranularity.QUARTER:
            if date_to_adjust.month <= 3:
                return date_to_adjust + relativedelta(month=1, day=1)
            elif date_to_adjust.month <= 6:
                return date_to_adjust + relativedelta(month=4, day=1)
            elif date_to_adjust.month <= 9:
                return date_to_adjust + relativedelta(month=7, day=1)
            else:
                return date_to_adjust + relativedelta(month=10, day=1)
        elif time_granularity is TimeGranularity.YEAR:
            return date_to_adjust + relativedelta(month=1, day=1)
        else:
            assert_values_exhausted(time_granularity)

    @override
    def adjust_to_end_of_period(
        self, time_granularity: TimeGranularity, date_to_adjust: datetime.datetime
    ) -> datetime.datetime:
        """Takes a date and adjusts it to the end of the given granularity period.

        Used to adjust time constraints when the time dimension they will be applied to has a larger granularity
        than the time constraint requested.

        Args:
            time_granularity: The granularity of the time dimension that will be constrained.
            date_to_adjust: The time constraint end date.
        """
        # Strip time data that is irrelevant to avoid rendering an unnecessarily complex time constraint.
        date_to_adjust = date_to_adjust.replace(microsecond=0)
        if time_granularity.to_int() >= TimeGranularity.DAY.to_int():
            date_to_adjust = date_to_adjust.replace(hour=0, minute=0, second=0)

        if (
            time_granularity is TimeGranularity.NANOSECOND
            or time_granularity is TimeGranularity.MICROSECOND
            or time_granularity is TimeGranularity.MILLISECOND
        ):
            raise ValueError("Time constraints only support SECOND and larger time granularities.")
        elif time_granularity is TimeGranularity.SECOND:
            return date_to_adjust
        elif time_granularity is TimeGranularity.MINUTE:
            return date_to_adjust + relativedelta(second=59)
        elif time_granularity is TimeGranularity.HOUR:
            return date_to_adjust + relativedelta(minute=59, second=59)
        elif time_granularity is TimeGranularity.DAY:
            return date_to_adjust
        elif time_granularity is TimeGranularity.WEEK:
            return date_to_adjust + relativedelta(weekday=dateutil.relativedelta.SU(1))
        elif time_granularity is TimeGranularity.MONTH:
            return date_to_adjust + relativedelta(day=31)
        elif time_granularity is TimeGranularity.QUARTER:
            if date_to_adjust.month <= 3:
                return date_to_adjust + relativedelta(month=3, day=31)
            elif date_to_adjust.month <= 6:
                return date_to_adjust + relativedelta(month=6, day=31)
            elif date_to_adjust.month <= 9:
                return date_to_adjust + relativedelta(month=9, day=31)
            else:
                return date_to_adjust + relativedelta(month=12, day=31)
        elif time_granularity is TimeGranularity.YEAR:
            return date_to_adjust + relativedelta(month=12, day=31)
        else:
            assert_values_exhausted(time_granularity)

    @override
    def expand_time_constraint_for_cumulative_metric(
        self, time_constraint: TimeRangeConstraint, granularity: Optional[TimeGranularity], count: int
    ) -> TimeRangeConstraint:
        if granularity is not None:
            return TimeRangeConstraint(
                start_time=time_constraint.start_time - self._relative_delta_for_window(granularity, count),
                end_time=time_constraint.end_time,
            )

        # if no window is specified we want to accumulate from the beginning of time
        return TimeRangeConstraint(
            start_time=TimeRangeConstraint.ALL_TIME_BEGIN(),
            end_time=time_constraint.end_time,
        )
