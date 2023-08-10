from __future__ import annotations

from metricflow.specs.query_interface import QueryInterfaceDimension, QueryInterfaceDimensionFactory


class GroupByDimension(QueryInterfaceDimension):
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
    def create(self, name: str) -> GroupByDimension:
        return GroupByDimension(name)
