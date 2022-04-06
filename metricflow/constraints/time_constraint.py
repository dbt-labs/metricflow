from __future__ import annotations

import datetime
from typing import Optional

import pandas as pd

from metricflow.model.objects.utils import HashableBaseModel
from metricflow.time.time_granularity import TimeGranularity


class TimeRangeConstraint(HashableBaseModel):
    """Describes how the time dimension for metrics should be constrained."""

    start_time: datetime.datetime
    end_time: datetime.datetime

    @staticmethod
    def ALL_TIME_BEGIN() -> datetime.datetime:  # noqa: D
        return datetime.datetime(2000, 1, 1)

    @staticmethod
    def ALL_TIME_END() -> datetime.datetime:  # noqa: D
        return datetime.datetime(2040, 12, 31)

    @staticmethod
    def all_time() -> TimeRangeConstraint:
        """Return the range representing all time.

        This could also be represented with None as the ends, but doing this makes the logic simpler in many cases.
        """
        return TimeRangeConstraint(
            start_time=TimeRangeConstraint.ALL_TIME_BEGIN(),
            end_time=TimeRangeConstraint.ALL_TIME_END(),
        )

    @staticmethod
    def empty_time() -> TimeRangeConstraint:
        """Return the range representing no time"""
        return TimeRangeConstraint(
            start_time=TimeRangeConstraint.ALL_TIME_BEGIN(),
            end_time=TimeRangeConstraint.ALL_TIME_BEGIN(),
        )

    def _adjust_time_constraint_start_by_window(
        self,
        time_granularity: TimeGranularity,
        time_unit_count: int,
    ) -> TimeRangeConstraint:
        """Moves the start of the time constraint back by 1 window

        if the metric is weekly-active-users (ie window = 1 week) it moves time_constraint.start one week earlier
        """
        start_ts = pd.Timestamp(self.start_time)
        offset = time_granularity.offset_period * time_unit_count
        adjusted_start = (start_ts - offset).to_pydatetime()
        return TimeRangeConstraint(
            start_time=adjusted_start,
            end_time=self.end_time,
        )

    def adjust_time_constraint_for_cumulative_metric(
        self, granularity: Optional[TimeGranularity], count: int
    ) -> TimeRangeConstraint:
        """Given a time constraint for the overall query, adjust it to cover the time range for this metric."""
        if granularity is not None:
            return self._adjust_time_constraint_start_by_window(granularity, count - 1)

        # if no window is specified we want to accumulate from the beginning of time
        return TimeRangeConstraint(
            start_time=TimeRangeConstraint.ALL_TIME_BEGIN(),
            end_time=self.end_time,
        )

    def is_subset_of(self, other: TimeRangeConstraint) -> bool:  # noqa: D
        return self.start_time >= other.start_time and self.end_time <= other.end_time

    def __str__(self) -> str:  # noqa: D
        return f"[{self.start_time.isoformat()}, {self.end_time.isoformat()}]"

    def __repr__(self) -> str:  # noqa: D
        return (
            f"{self.__class__.__name__}(start_time='{self.start_time.isoformat()}', "
            f"end_time='{self.end_time.isoformat()}')"
        )

    def intersection(self, other: TimeRangeConstraint) -> TimeRangeConstraint:  # noqa: D
        # self is completely before the other
        if self.end_time < other.start_time:
            return TimeRangeConstraint.empty_time()
        # self starts before the start of other, and self ends within other
        elif self.start_time <= other.start_time <= self.end_time <= other.end_time:
            return TimeRangeConstraint(
                start_time=other.start_time,
                end_time=self.end_time,
            )
        # self starts before the start of other, and self ends after other
        elif self.start_time <= other.start_time <= other.end_time <= self.end_time:
            return other
        # self starts after the start of other, and self ends within other:
        elif other.start_time <= self.start_time <= self.end_time <= other.end_time:
            return self
        # self starts after the start of other, and self ends after other:
        elif other.start_time <= self.start_time <= other.end_time <= self.end_time:
            return TimeRangeConstraint(
                start_time=self.start_time,
                end_time=other.end_time,
            )
        # self is completely after other
        elif self.start_time > other.end_time:
            return TimeRangeConstraint.empty_time()
        else:
            raise RuntimeError(f"Unhandled case - self: {self} other: {other}")
