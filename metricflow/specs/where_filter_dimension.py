from __future__ import annotations

from typing import List, Optional, Sequence, Union

from dbt_semantic_interfaces.call_parameter_sets import (
    FilterCallParameterSets,
)
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dbt_semantic_interfaces.protocols.query_interface import (
    QueryInterfaceDimension,
    QueryInterfaceDimensionFactory,
)
from dbt_semantic_interfaces.type_enums import TimeGranularity
from typing_extensions import override

from metricflow.errors.errors import InvalidQuerySyntax
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.dimension_spec_resolver import DimensionSpecResolver
from metricflow.specs.specs import DimensionSpec, TimeDimensionSpec


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
        self._column_association_resolver = column_association_resolver
        self._name = name
        self._entity_path = entity_path
        self._time_granularity_name: Optional[str] = None
        self._date_part_name: Optional[str] = None

    def grain(self, time_granularity_name: str) -> QueryInterfaceDimension:
        """The time granularity."""
        self._time_granularity_name = time_granularity_name
        return self

    def date_part(self, date_part_name: str) -> QueryInterfaceDimension:
        """The date_part requested to extract."""
        self._date_part_name = date_part_name
        return self

    def descending(self, _is_descending: bool) -> QueryInterfaceDimension:
        """Set the sort order for order-by."""
        raise InvalidQuerySyntax("descending is invalid in the where parameter")

    @property
    def is_time_dimension(self) -> bool:
        return bool(self._time_granularity_name or self._date_part_name)

    @property
    def time_dimension_spec(self) -> TimeDimensionSpec:
        return self._dimension_spec_resolver.resolve_time_dimension_spec(
            self._name, self._time_granularity_name, self._entity_path, self._date_part_name
        )

    @property
    def dimension_spec(self) -> DimensionSpec:
        return self._dimension_spec_resolver.resolve_dimension_spec(self._name, self._entity_path)

    def __str__(self) -> str:
        """Returns the column name.

        Important in the Jinja sandbox.
        """
        return self._column_association_resolver.resolve_spec(
            self.time_dimension_spec if self.is_time_dimension else self.dimension_spec
        ).column_name


class WhereFilterDimensionFactory(ProtocolHint[QueryInterfaceDimensionFactory]):
    """Creates a WhereFilterDimension.

    Each call to `create` adds a WhereFilterDimension to created.
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
