from __future__ import annotations

from typing import Optional, Protocol, Sequence


class QueryInterfaceMetric(Protocol):
    """Represents the interface for Metric in the query interface."""

    def descending(self, _is_descending: bool) -> QueryInterfaceMetric:
        """Set the sort order for order-by."""
        raise NotImplementedError


class QueryInterfaceDimension(Protocol):
    """Represents the interface for Dimension in the query interface."""

    def grain(self, _grain: str) -> QueryInterfaceDimension:
        """The time granularity."""
        raise NotImplementedError

    def alias(self, _alias: str) -> QueryInterfaceDimension:
        """Renaming the column."""
        raise NotImplementedError

    def descending(self, _is_descending: bool) -> QueryInterfaceDimension:
        """Set the sort order for order-by."""

    def date_part(self, _date_part: str) -> QueryInterfaceDimension:
        """Date part to extract from the dimension."""
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
        descending: bool = False,
        date_part_name: Optional[str] = None,
        entity_path: Sequence[str] = (),
    ) -> QueryInterfaceTimeDimension:
        """Create a TimeDimension."""
        raise NotImplementedError


class QueryInterfaceEntity(Protocol):
    """Represents the interface for Entity in the query interface."""

    def descending(self, _is_descending: bool) -> QueryInterfaceEntity:
        """Set the sort order for order-by."""
        raise NotImplementedError


class QueryInterfaceEntityFactory(Protocol):
    """Creates an Entity for the query interface.

    Represented as the Entity constructor in the Jinja sandbox.
    """

    def create(self, entity_name: str, entity_path: Sequence[str] = ()) -> QueryInterfaceEntity:
        """Create an Entity."""
        raise NotImplementedError
