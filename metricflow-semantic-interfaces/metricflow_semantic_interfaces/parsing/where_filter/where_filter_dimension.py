from __future__ import annotations

from typing import List, Optional, Sequence

from metricflow_semantic_interfaces.errors import InvalidQuerySyntax
from metricflow_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from metricflow_semantic_interfaces.protocols.query_interface import (
    QueryInterfaceDimension,
    QueryInterfaceDimensionFactory,
)
from typing_extensions import override


class WhereFilterDimension(ProtocolHint[QueryInterfaceDimension]):
    """A dimension that is passed in through the where filter parameter."""

    @override
    def _implements_protocol(self) -> QueryInterfaceDimension:
        return self

    def __init__(  # noqa
        self,
        name: str,
        entity_path: Sequence[str],
    ) -> None:
        self.name = name
        self.entity_path = entity_path
        self.time_granularity_name: Optional[str] = None
        self.date_part_name: Optional[str] = None

    def grain(self, time_granularity: str) -> QueryInterfaceDimension:
        """The time granularity."""
        self.time_granularity_name = time_granularity
        return self

    def descending(self, _is_descending: bool) -> QueryInterfaceDimension:
        """Set the sort order for order-by."""
        raise InvalidQuerySyntax("descending is invalid in the where parameter and filter spec")

    def date_part(self, date_part_name: str) -> QueryInterfaceDimension:
        """Date part to extract from the dimension."""
        self.date_part_name = date_part_name
        return self


class WhereFilterDimensionFactory(ProtocolHint[QueryInterfaceDimensionFactory]):
    """Creates a WhereFilterDimension.

    Each call to `create` adds a WhereFilterDimension to `created`.
    """

    @override
    def _implements_protocol(self) -> QueryInterfaceDimensionFactory:
        return self

    def __init__(self) -> None:  # noqa
        self.created: List[WhereFilterDimension] = []

    def create(self, dimension_name: str, entity_path: Sequence[str] = ()) -> WhereFilterDimension:
        """Gets called by Jinja when rendering {{ Dimension(...) }}."""
        dimension = WhereFilterDimension(dimension_name, entity_path)
        self.created.append(dimension)
        return dimension
