from __future__ import annotations

from typing import Sequence
from abc import ABC


class QueryInterfaceMetric(ABC):
    """Metric in the Jinja interface."""

    def __init__(self, name: str) -> None:  # noqa: D
        raise NotImplementedError

    def pct_growth(self) -> QueryInterfaceMetric:
        """The percentage growth."""
        raise NotImplementedError


class QueryInterfaceDimension(ABC):
    """Dimension in the Jinja interface."""

    def __init__(self, name: str, entity_path: Sequence[str] = ()) -> None:  # noqa: D
        raise NotImplementedError

    def grain(self, _grain: str) -> QueryInterfaceDimension:
        """The time granularity."""
        raise NotImplementedError

    def alias(self, _alias: str) -> QueryInterfaceDimension:
        """Renaming the column."""
        raise NotImplementedError


class QueryInterfaceTimeDimension(ABC):
    """Time Dimension in a where clause with Jinja."""

    def __init__(  # noqa
        self,
        time_dimension_name: str,
        time_granularity_name: str,
        entity_path: Sequence[str] = (),
    ):
        raise NotImplementedError


class QueryInterfaceEntity(ABC):
    """Entity in a where clause with Jinja."""

    def __init__(self, entity_name: str, entity_path: Sequence[str] = ()):  # noqa
        raise NotImplementedError
