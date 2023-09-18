from __future__ import annotations

from typing import Optional, Protocol

from dbt_semantic_interfaces.type_enums import TimeGranularity


class QueryParameterDimension(Protocol):
    """A query parameter with a grain."""

    @property
    def name(self) -> str:
        """The name of the item."""
        raise NotImplementedError

    @property
    def grain(self) -> Optional[TimeGranularity]:
        """The time granularity."""
        raise NotImplementedError

    @property
    def descending(self) -> bool:
        """Set the sort order for order-by."""
        raise NotImplementedError


class QueryParameterMetric(Protocol):
    """Metric in the query interface."""

    @property
    def name(self) -> str:
        """The name of the metric."""
        raise NotImplementedError

    @property
    def descending(self) -> bool:
        """Set the sort order for order-by."""
        raise NotImplementedError
