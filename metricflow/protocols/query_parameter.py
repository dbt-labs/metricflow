from __future__ import annotations

from typing import Optional, Protocol, Union, runtime_checkable

from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow.time.date_part import DatePart


@runtime_checkable
class MetricQueryParameter(Protocol):
    """Metric requested in a query."""

    @property
    def name(self) -> str:
        """The name of the metric."""
        raise NotImplementedError


@runtime_checkable
class DimensionOrEntityQueryParameter(Protocol):
    """Generic group by parameter for queries. Might be an entity or a dimension."""

    @property
    def name(self) -> str:
        """The name of the metric."""
        raise NotImplementedError


@runtime_checkable
class TimeDimensionQueryParameter(Protocol):  # noqa: D
    @property
    def name(self) -> str:
        """The name of the item."""
        raise NotImplementedError

    @property
    def grain(self) -> Optional[TimeGranularity]:
        """The time granularity."""
        raise NotImplementedError

    @property
    def date_part(self) -> Optional[DatePart]:
        """Date part to extract from the dimension."""
        raise NotImplementedError


GroupByParameter = Union[DimensionOrEntityQueryParameter, TimeDimensionQueryParameter]
InputOrderByParameter = Union[MetricQueryParameter, GroupByParameter]


class OrderByQueryParameter(Protocol):
    """Parameter to order by, specifying ascending or descending."""

    @property
    def order_by(self) -> InputOrderByParameter:
        """Parameter to order results by."""
        raise NotImplementedError

    @property
    def descending(self) -> bool:
        """Indicates if the order should be ascending or descending."""
        raise NotImplementedError
