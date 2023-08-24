from __future__ import annotations

from typing import List, Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    FilterCallParameterSets,
)
from dbt_semantic_interfaces.naming.dundered import DunderedNameFormatter
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
)
from typing_extensions import override

from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.query_interface import (
    QueryInterfaceDimension,
    QueryInterfaceDimensionFactory,
)
from metricflow.specs.specs import DimensionSpec


class WhereFilterDimension(ProtocolHint[QueryInterfaceDimension]):
    """A dimension that is passed in through the where filter parameter."""

    @override
    def _implements_protocol(self) -> QueryInterfaceDimension:
        return self

    def __init__(self, column_name: str) -> None:  # noqa
        self.column_name = column_name

    def grain(self, _grain: str) -> QueryInterfaceDimension:
        """The time granularity."""
        raise NotImplementedError

    def alias(self, _alias: str) -> QueryInterfaceDimension:
        """Renaming the column."""
        raise NotImplementedError

    def __str__(self) -> str:
        """Returns the column name.

        Important in the Jinja sandbox.
        """
        return self.column_name


class WhereFilterDimensionFactory(ProtocolHint[QueryInterfaceDimensionFactory]):
    """Creates a WhereFilterDimension.

    Each call to `create` adds a DimensionSpec to dimension_specs.
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
        self.dimension_specs: List[DimensionSpec] = []

    def create(self, name: str, entity_path: Sequence[str] = ()) -> WhereFilterDimension:
        """Create a WhereFilterDimension."""
        structured_name = DunderedNameFormatter.parse_name(name)
        call_parameter_set = DimensionCallParameterSet(
            dimension_reference=DimensionReference(element_name=structured_name.element_name),
            entity_path=(
                tuple(EntityReference(element_name=arg) for arg in entity_path) + structured_name.entity_links
            ),
        )
        assert call_parameter_set in self._call_parameter_sets.dimension_call_parameter_sets

        dimension_spec = self._convert_to_dimension_spec(call_parameter_set)
        self.dimension_specs.append(dimension_spec)
        column_name = self._column_association_resolver.resolve_spec(dimension_spec).column_name
        return WhereFilterDimension(column_name)

    def _convert_to_dimension_spec(
        self,
        parameter_set: DimensionCallParameterSet,
    ) -> DimensionSpec:  # noqa: D
        return DimensionSpec(
            element_name=parameter_set.dimension_reference.element_name,
            entity_links=parameter_set.entity_path,
        )
