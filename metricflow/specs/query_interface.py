from __future__ import annotations

from typing import Sequence, Protocol


class QueryInterfaceDimensionFactory(Protocol):
    """Creates a Dimension for the query interface.

    Represented as the Dimension constructor in the Jinja sandbox.
    """

    def create(self, name: str, entity_path: Sequence[str] = ()) -> QueryInterfaceDimension:
        raise NotImplementedError


class QueryInterfaceDimension(Protocol):
    """Dimension for the query interface."""

    def grain(self, _grain: str) -> QueryInterfaceDimension:
        """The time granularity."""
        raise NotImplementedError

    def alias(self, _alias: str) -> QueryInterfaceDimension:
        """Renaming the column."""
        raise NotImplementedError


class QueryInterfaceTimeDimensionFactory(Protocol):
    def create(
        self,
        time_dimension_name: str,
        time_granularity_name: str,
        entity_path: Sequence[str] = (),
    ) -> QueryInterfaceTimeDimension:
        raise NotImplementedError


class QueryInterfaceTimeDimension(Protocol):
    """Time Dimension for the query interface."""

    pass


class QueryInterfaceEntityFactory(Protocol):
    def create(self, entity_name: str, entity_path: Sequence[str] = ()) -> QueryInterfaceEntity:
        raise NotImplementedError


class QueryInterfaceEntity(Protocol):
    """Entity for the query interface."""

    pass
