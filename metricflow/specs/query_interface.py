from __future__ import annotations

from typing import Protocol, Sequence


class QueryInterfaceMetric:
    """Metric in the query interface."""

    def __init__(self, name: str) -> None:  # noqa: D
        self.name = name

    def pct_growth(self) -> QueryInterfaceMetric:
        """The percentage growth."""
        raise NotImplementedError("percent growth is not implemented yet")

    def __str__(self) -> str:
        """The Metric's name."""
        return self.name


class QueryParameter(Protocol):
    """Represents the interface for Dimension, TimeDimension, and Entity parameters in the query interface."""

    def grain(self, _grain: str) -> QueryParameter:
        """The time granularity."""
        raise NotImplementedError

    def alias(self, _alias: str) -> QueryParameter:
        """Renaming the column."""
        raise NotImplementedError


class QueryInterfaceDimensionFactory(Protocol):
    """Creates a Dimension for the query interface.

    Represented as the Dimension constructor in the Jinja sandbox.
    """

    def create(self, name: str, entity_path: Sequence[str] = ()) -> QueryParameter:
        """Create a QueryInterfaceDimension."""
        raise NotImplementedError


class QueryInterfaceTimeDimensionFactory(Protocol):
    """Creates a TimeDimension for the query interface.

    Represented as the TimeDimension constructor in the Jinja sandbox.
    """

    def create(
        self,
        time_dimension_name: str,
        time_granularity_name: str,
        entity_path: Sequence[str] = (),
    ) -> QueryParameter:
        """Create a TimeDimension."""
        raise NotImplementedError


class QueryInterfaceEntityFactory(Protocol):
    """Creates an Entity for the query interface.

    Represented as the Entity constructor in the Jinja sandbox.
    """

    def create(self, entity_name: str, entity_path: Sequence[str] = ()) -> QueryParameter:
        """Create an Entity."""
        raise NotImplementedError
