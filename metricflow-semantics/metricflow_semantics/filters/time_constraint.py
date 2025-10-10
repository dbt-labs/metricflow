from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass

from metricflow_semantics.errors.error_classes import UnableToSatisfyQueryError
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TimeRangeConstraint(SerializableDataclass):
    """Describes how the time dimension for metrics should be constrained."""

    start_time: datetime.datetime
    end_time: datetime.datetime

    def __post_init__(self) -> None:  # noqa: D105
        if self.start_time > self.end_time:
            logger.warning(
                LazyFormat(
                    lambda: f"start_time must not be > end_time. start_time={self.start_time} end_time={self.end_time}"
                )
            )

        if self.start_time < TimeRangeConstraint.ALL_TIME_BEGIN():
            logger.warning(
                LazyFormat(
                    lambda: f"start_time={self.start_time} exceeds the limits of {TimeRangeConstraint.ALL_TIME_BEGIN()}"
                )
            )

        if self.end_time > TimeRangeConstraint.ALL_TIME_END():
            raise UnableToSatisfyQueryError(
                f"end_time={self.end_time} exceeds the limits of {TimeRangeConstraint.ALL_TIME_END()}"
            )

    @staticmethod
    def ALL_TIME_BEGIN() -> datetime.datetime:  # noqa: D102
        return datetime.datetime(2000, 1, 1)

    @staticmethod
    def ALL_TIME_END() -> datetime.datetime:  # noqa: D102
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
        """Return the range representing no time."""
        return TimeRangeConstraint(
            start_time=TimeRangeConstraint.ALL_TIME_BEGIN(),
            end_time=TimeRangeConstraint.ALL_TIME_BEGIN(),
        )

    def is_subset_of(self, other: TimeRangeConstraint) -> bool:  # noqa: D102
        return self.start_time >= other.start_time and self.end_time <= other.end_time

    def __str__(self) -> str:  # noqa: D105
        return f"[{self.start_time.isoformat()}, {self.end_time.isoformat()}]"

    def __repr__(self) -> str:  # noqa: D105
        return (
            f"{self.__class__.__name__}(start_time='{self.start_time.isoformat()}', "
            f"end_time='{self.end_time.isoformat()}')"
        )

    def intersection(self, other: TimeRangeConstraint) -> TimeRangeConstraint:  # noqa: D102
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
