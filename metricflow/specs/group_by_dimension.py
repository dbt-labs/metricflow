from __future__ import annotations

from typing import Sequence

from metricflow.specs.query_interface import QueryInterfaceDimension, QueryInterfaceDimensionFactory


class GroupByDimension(QueryInterfaceDimension):
    """A dimension that is passed in through the group_by parameter."""

    def __init__(self, name: str) -> None:  # noqa: D
        self.name = name

    def grain(self, _grain: str) -> GroupByDimension:
        """The time granularity."""
        self._grain = _grain
        return self

    def alias(self, _alias: str) -> GroupByDimension:
        """Renaming the column."""
        raise NotImplementedError("alias is not implemented yet")

    def __str__(self) -> str:
        """Returns in dunder-format if necessary."""
        return f"{self.name}__{self._grain}" if self._grain else self.name


class GroupByDimensionFactory(QueryInterfaceDimensionFactory):
    """Creates a GroupByDimension.

    This is useful as a factory to type checking for the Jinja sandbox.
    """

    def create(self, name: str, entity_path: Sequence[str] = ()) -> GroupByDimension:
        """Create a GroupByDimension."""
        return GroupByDimension(name)
