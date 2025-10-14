from __future__ import annotations

from abc import abstractmethod
from typing import Optional, Protocol, Sequence


class QueryInterfaceMetric(Protocol):
    """Represents the interface for Metric in the query interface."""

    @abstractmethod
    def descending(self, _is_descending: bool) -> QueryInterfaceMetric:
        """Set the sort order for order-by."""
        pass


class QueryInterfaceDimension(Protocol):
    """Represents the interface for Dimension in the query interface."""

    @abstractmethod
    def grain(self, _grain: str) -> QueryInterfaceDimension:
        """The time granularity."""
        pass

    @abstractmethod
    def descending(self, _is_descending: bool) -> QueryInterfaceDimension:
        """Set the sort order for order-by."""
        pass

    @abstractmethod
    def date_part(self, _date_part: str) -> QueryInterfaceDimension:
        """Date part to extract from the dimension."""
        pass


class QueryInterfaceDimensionFactory(Protocol):
    """Creates a Dimension for the query interface.

    Represented as the Dimension constructor in the Jinja sandbox.
    """

    @abstractmethod
    def create(self, name: str, entity_path: Sequence[str] = ()) -> QueryInterfaceDimension:
        """Create a QueryInterfaceDimension."""
        pass


class QueryInterfaceTimeDimension(Protocol):
    """Represents the interface for TimeDimension in the query interface."""

    pass


class QueryInterfaceTimeDimensionFactory(Protocol):
    """Creates a TimeDimension for the query interface.

    Represented as the TimeDimension constructor in the Jinja sandbox.
    """

    @abstractmethod
    def create(
        self,
        time_dimension_name: str,
        time_granularity_name: Optional[str] = None,
        entity_path: Sequence[str] = (),
        descending: Optional[bool] = None,
        date_part_name: Optional[str] = None,
    ) -> QueryInterfaceTimeDimension:
        """Create a TimeDimension."""
        pass


class QueryInterfaceEntity(Protocol):
    """Represents the interface for Entity in the query interface."""

    pass


class QueryInterfaceEntityFactory(Protocol):
    """Creates an Entity for the query interface.

    Represented as the Entity constructor in the Jinja sandbox.
    """

    @abstractmethod
    def create(self, entity_name: str, entity_path: Sequence[str] = ()) -> QueryInterfaceEntity:
        """Create an Entity."""
        pass


class QueryInterfaceMetricFactory(Protocol):
    """Creates an Metric for the query interface.

    Represented as the Metric constructor in the Jinja sandbox.
    """

    @abstractmethod
    def create(self, metric_name: str, group_by: Sequence[str] = ()) -> QueryInterfaceMetric:
        """Create a Metric."""
        pass
