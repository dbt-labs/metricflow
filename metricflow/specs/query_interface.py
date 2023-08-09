from __future__ import annotations

from typing import Sequence, Protocol
from abc import abstractmethod


class QueryInterfaceMetric(Protocol):
    """Metric for the query  interface."""

    def __init__(self, name: str) -> None:  # noqa: D
        raise NotImplementedError


class QueryInterfaceDimensionFactory(Protocol):
    """Creates a Dimension for the query interface.

    Represented as the Dimension constructor in the Jinja sandbox.
    """

    def create(self, name: str, entity_path: Sequence[str] = ()) -> QueryInterfaceDimension:
        """Creates a"""
        raise NotImplementedError


class QueryInterfaceDimension(Protocol):
    """Dimension for the query interface."""

    @abstractmethod
    def grain(self, _grain: str) -> QueryInterfaceDimension:
        """The time granularity."""
        raise NotImplementedError


class QueryInterfaceTimeDimension(Protocol):
    """Time Dimension for the query interface."""

    def __init__(  # noqa
        self,
        time_dimension_name: str,
        time_granularity_name: str,
        entity_path: Sequence[str] = (),
    ):
        raise NotImplementedError


class QueryInterfaceEntity(Protocol):
    """Entity for the query interface."""

    def __init__(self, entity_name: str, entity_path: Sequence[str] = ()):  # noqa
        raise NotImplementedError
