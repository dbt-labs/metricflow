from __future__ import annotations

from abc import abstractmethod
from typing import Protocol

from metricflow_semantic_interfaces.type_enums import TimeGranularity


class TimeSpineTableConfiguration(Protocol):
    """Legacy time spine class that will eventually be deprecated in favor of TimeSpine.

    Describes the configuration for a time spine table.
    A time spine table is a table with a single column containing dates at a specific grain.
    e.g. with day granularity:
    ...
    2020-01-01
    2020-01-02
    2020-01-03
    ...

    The time spine table is used to join to the measure source to compute cumulative metrics.
    """

    @property
    @abstractmethod
    def location(self) -> str:
        """The location of the time spine table in schema_name.table_name format."""
        pass

    @property
    @abstractmethod
    def column_name(self) -> str:
        """The name of the column in the time spine table that has the date values."""

    @property
    @abstractmethod
    def grain(self) -> TimeGranularity:
        """The grain of the dates in the time spine table."""
        pass
