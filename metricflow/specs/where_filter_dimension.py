from __future__ import annotations

from typing import List, Optional, Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    FilterCallParameterSets,
)
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dbt_semantic_interfaces.type_enums import TimeGranularity

from typing_extensions import override

from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.dimension_spec_resolver import DimensionSpecResolver
from metricflow.specs.query_interface import (
    QueryInterfaceDimension,
    QueryInterfaceDimensionFactory,
)


class WhereFilterDimension(ProtocolHint[QueryInterfaceDimension]):
    """A dimension that is passed in through the where filter parameter."""

    @override
    def _implements_protocol(self) -> QueryInterfaceDimension:
        return self

    def __init__(  # noqa
        self,
        name: str,
        entity_path: Sequence[str],
        call_parameter_sets: FilterCallParameterSets,
        column_association_resolver: ColumnAssociationResolver,
    ) -> None:
        self._dimension_spec_resolver = DimensionSpecResolver(call_parameter_sets)
        self.name = name
        self.spec = self._dimension_spec_resolver.resolve_dimension_spec(name, entity_path)
        self._column_association_resolver = column_association_resolver
        self.time_granularity: Optional[TimeGranularity] = None
        self.entity_path = entity_path

    def grain(self, time_granularity_name: str) -> QueryInterfaceDimension:
        """The time granularity."""
        self.time_granularity = TimeGranularity(time_granularity_name)
        self.spec = self._dimension_spec_resolver.resolve_time_dimension_spec(
            self.name, self.time_granularity, self.entity_path
        )

    def alias(self, _alias: str) -> QueryInterfaceDimension:
        """Renaming the column."""
        raise NotImplementedError

    def __str__(self) -> str:
        """Returns the column name.

        Important in the Jinja sandbox.
        """
        return self._column_association_resolver.resolve_spec(self.spec).column_name


class WhereFilterDimensionFactory(ProtocolHint[QueryInterfaceDimensionFactory]):
    """Creates a WhereFilterDimension.

    Each call to `create` adds a DimensionSpec to created.
    """

    @override
    def _implements_protocol(self) -> QueryInterfaceDimensionFactory:
        return self

    def __init__(  # noqa
        self,
        call_parameter_sets: FilterCallParameterSets,
        column_association_resolver: ColumnAssociationResolver,
    ):
        self._call_parameter_sets = call_parameter_sets
        self._column_association_resolver = column_association_resolver
        self.created: List[WhereFilterDimension] = []

    def create(self, name: str, entity_path: Sequence[str] = ()) -> WhereFilterDimension:
        """Create a WhereFilterDimension."""
        dimension = WhereFilterDimension(
            name, entity_path, self._call_parameter_sets, self._column_association_resolver
        )
        self.created.append(dimension)
        return dimension
