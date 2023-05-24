from __future__ import annotations

import datetime as dt

from metricflow.time.time_source import TimeSource


class ServerTimeSource(TimeSource):
    """A time source that represents the current datetime in UTC."""

    def get_time(self) -> dt.datetime:  # noqa: D
        return dt.datetime.utcnow()
