from __future__ import annotations

from typing import Optional, Protocol, Sequence

from dbt_semantic_interfaces.type_enums import TimeGranularity


class QueryInterfaceMetric(Protocol):
    """Metric in the query interface."""

    @property
    def name(self) -> str:
        """The name of the metric."""
        raise NotImplementedError


class QueryParameter(Protocol):
    """A query parameter with a grain."""

    @property
    def name(self) -> str:
        """The name of the item."""
        raise NotImplementedError

    @property
    def grain(self) -> Optional[TimeGranularity]:
        """The time granularity."""
        raise NotImplementedError


class QueryInterfaceDimension(Protocol):
    """Represents the interface for Dimension in the query interface."""

    def grain(self, _grain: str) -> QueryInterfaceDimension:
        """The time granularity."""
        raise NotImplementedError

    def alias(self, _alias: str) -> QueryInterfaceDimension:
        """Renaming the column."""
        raise NotImplementedError


class QueryInterfaceDimensionFactory(Protocol):
    """Creates a Dimension for the query interface.

    Represented as the Dimension constructor in the Jinja sandbox.
    """

    def create(self, name: str, entity_path: Sequence[str] = ()) -> QueryInterfaceDimension:
        """Create a QueryInterfaceDimension."""
        raise NotImplementedError


class QueryInterfaceTimeDimension(Protocol):
    """Represents the interface for TimeDimension in the query interface."""

    pass


class QueryInterfaceTimeDimensionFactory(Protocol):
    """Creates a TimeDimension for the query interface.

    Represented as the TimeDimension constructor in the Jinja sandbox.
    """

    def create(
        self,
        time_dimension_name: str,
        time_granularity_name: str,
        entity_path: Sequence[str] = (),
    ) -> QueryInterfaceTimeDimension:
        """Create a TimeDimension."""
        raise NotImplementedError


class QueryInterfaceEntity(Protocol):
    """Represents the interface for Entity in the query interface."""

    pass


class QueryInterfaceEntityFactory(Protocol):
    """Creates an Entity for the query interface.

    Represented as the Entity constructor in the Jinja sandbox.
    """

    def create(self, entity_name: str, entity_path: Sequence[str] = ()) -> QueryInterfaceEntity:
        """Create an Entity."""
        raise NotImplementedError
