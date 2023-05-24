from __future__ import annotations

import datetime

from metricflow.time.time_source import TimeSource


class ConfigurableTimeSource(TimeSource):
    """A time source that can be configured so that scheduled operations can be simulated in testing."""

    def __init__(self, configured_time: datetime.datetime) -> None:  # noqa: D
        self._configured_time = configured_time

    def get_time(self) -> datetime.datetime:  # noqa: D
        return self._configured_time

    def set_time(self, new_time: datetime.datetime) -> datetime.datetime:  # noqa: D
        self._configured_time = new_time
        return new_time
