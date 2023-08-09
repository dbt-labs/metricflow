from __future__ import annotations

from typing import Sequence
from abc import ABC


class JinjaSyntaxMetric(ABC):
    """Metric in the Jinja interface."""

    def __init__(self, name: str) -> None:  # noqa: D
        raise NotImplementedError

    def pct_growth(self) -> JinjaSyntaxMetric:
        """The percentage growth."""
        raise NotImplementedError


class JinjaSyntaxDimension(ABC):
    """Dimension in the Jinja interface."""

    def __init__(self, name: str, entity_path: Sequence[str] = ()) -> None:  # noqa: D
        raise NotImplementedError

    def grain(self, _grain: str) -> JinjaSyntaxDimension:
        """The time granularity."""
        raise NotImplementedError

    def alias(self, _alias: str) -> JinjaSyntaxDimension:
        """Renaming the column."""
        raise NotImplementedError


class JinjaSyntaxTimeDimension(ABC):
    """Time Dimension in a where clause with Jinja."""

    def __init__(  # noqa
        self,
        time_dimension_name: str,
        time_granularity_name: str,
        entity_path: Sequence[str] = (),
    ):
        raise NotImplementedError


class JinjaSyntaxEntity(ABC):
    """Entity in a where clause with Jinja."""

    def __init__(self, entity_name: str, entity_path: Sequence[str] = ()):  # noqa
        raise NotImplementedError
