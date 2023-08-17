from __future__ import annotations

from typing import Optional, Sequence

from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from typing_extensions import override

from metricflow.specs.query_interface import QueryInterfaceDimension, QueryInterfaceDimensionFactory


class GroupByOrderByDimension(ProtocolHint[QueryInterfaceDimension]):
    """A dimension that is passed in through the group_by or order_by parameter."""

    @override
    def _implements_protocol(self) -> QueryInterfaceDimension:
        return self

    def __init__(self, name: str, entity_path: Sequence[str] = ()) -> None:  # noqa: D
        self.name = name
        self.entity_path = entity_path
        self._grain: Optional[str] = None

    def grain(self, _grain: str) -> GroupByOrderByDimension:
        """The time granularity."""
        self._grain = _grain
        return self

    def alias(self, _alias: str) -> GroupByOrderByDimension:
        """Renaming the column."""
        raise NotImplementedError("alias is not implemented yet")

    def __str__(self) -> str:
        """Returns in dunder-format if necessary."""
        return f"{self.name}__{self._grain}" if self._grain else self.name


class GroupByOrderByDimensionFactory(ProtocolHint[QueryInterfaceDimensionFactory]):
    """Creates a GroupByOrderByDimension.

    This is useful as a factory for the Jinja sandbox.
    """

    @override
    def _implements_protocol(self) -> QueryInterfaceDimensionFactory:
        return self

    def create(self, name: str, entity_path: Sequence[str] = ()) -> GroupByOrderByDimension:
        """Create a GroupByOrderByDimension."""
        return GroupByOrderByDimension(name, entity_path)
