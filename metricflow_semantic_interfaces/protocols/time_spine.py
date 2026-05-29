from __future__ import annotations

from abc import abstractmethod
from typing import Optional, Protocol, Sequence

from metricflow_semantic_interfaces.implementations.node_relation import NodeRelation
from metricflow_semantic_interfaces.type_enums import TimeGranularity


class TimeSpine(Protocol):
    """Describes a table that contains dates at a specific time grain.

    One column must map to a standard granularity (one of the TimeGranularity enum members). Others might represent
    custom granularity columns. Custom granularity columns are not yet implemented.
    """

    @property
    @abstractmethod
    def node_relation(self) -> NodeRelation:
        """Dbt model where this time spine lives."""  # noqa: D102
        pass

    @property
    @abstractmethod
    def primary_column(self) -> TimeSpinePrimaryColumn:
        """The column in the time spine that maps to one of our standard granularities."""
        pass

    @property
    @abstractmethod
    def custom_granularities(self) -> Sequence[TimeSpineCustomGranularityColumn]:
        """The columns in the time spine table that map to custom granularities."""
        pass


class TimeSpinePrimaryColumn(Protocol):
    """The column in the time spine that maps to one of our standard granularities."""

    @property
    @abstractmethod
    def name(self) -> str:
        """The column name."""
        pass

    @property
    @abstractmethod
    def time_granularity(self) -> TimeGranularity:
        """The column name."""
        pass


class TimeSpineCustomGranularityColumn(Protocol):
    """A column in the time spine table that maps to a custom granularity."""

    @property
    @abstractmethod
    def name(self) -> str:
        """The column name."""
        pass

    @property
    @abstractmethod
    def column_name(self) -> Optional[str]:
        """The column name."""
        pass
