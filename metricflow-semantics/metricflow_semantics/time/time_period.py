from __future__ import annotations

import datetime
from abc import ABC, abstractmethod
from typing import Optional

from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow_semantics.filters.time_constraint import TimeRangeConstraint


class TimePeriodAdjuster(ABC):
    """Interface to simplify switching time-period adjustment-logic from `pandas` to `dateutil`."""

    @abstractmethod
    def adjust_to_start_of_period(
        self, time_granularity: TimeGranularity, date_to_adjust: datetime.datetime
    ) -> datetime.datetime:
        """Adjust to start of period if not at end already."""
        raise NotImplementedError

    @abstractmethod
    def adjust_to_end_of_period(
        self, time_granularity: TimeGranularity, date_to_adjust: datetime.datetime
    ) -> datetime.datetime:
        """Adjust to end of period if not at end already."""
        raise NotImplementedError

    @abstractmethod
    def expand_time_constraint_to_fill_granularity(
        self, time_constraint: TimeRangeConstraint, granularity: TimeGranularity
    ) -> TimeRangeConstraint:
        """Change the time range so that the ends are at the ends of the appropriate time granularity windows.

        e.g. [2020-01-15, 2020-2-15] with MONTH granularity -> [2020-01-01, 2020-02-29]
        """
        raise NotImplementedError

    @abstractmethod
    def expand_time_constraint_for_cumulative_metric(
        self, time_constraint: TimeRangeConstraint, granularity: Optional[TimeGranularity], count: int
    ) -> TimeRangeConstraint:
        """Moves the start of the time constraint back by <time_unit_count> windows for cumulative metrics.

        e.g. if the metric is weekly-active-users (window = 1 week) it moves time_constraint.start one week earlier
        """
        raise NotImplementedError
