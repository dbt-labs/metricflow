from __future__ import annotations

import datetime
from typing import Optional

import pandas as pd
from dbt_semantic_interfaces.type_enums import TimeGranularity
from typing_extensions import override

from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.time.time_granularity import (
    adjust_to_end_of_period,
    adjust_to_start_of_period,
    is_period_end,
    is_period_start,
    offset_period,
)
from metricflow_semantics.time.time_period import TimePeriodAdjuster


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
