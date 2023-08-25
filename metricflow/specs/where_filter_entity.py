from __future__ import annotations

from typing import List, Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    EntityCallParameterSet,
    FilterCallParameterSets,
)
from dbt_semantic_interfaces.naming.dundered import DunderedNameFormatter
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dbt_semantic_interfaces.references import EntityReference
from typing_extensions import override

from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.query_interface import QueryInterfaceDimension, QueryInterfaceEntityFactory
from metricflow.specs.specs import EntitySpec


class WhereFilterEntity(ProtocolHint[QueryInterfaceDimension]):
    """An entity that is passed in through the where filter parameter."""

    @override
    def _implements_protocol(self) -> QueryInterfaceDimension:
        return self

    def __init__(self, column_name: str):  # noqa
        self.column_name = column_name

    def grain(self, _grain: str) -> WhereFilterEntity:
        """The time granularity."""
        raise NotImplementedError

    def alias(self, _alias: str) -> WhereFilterEntity:
        """Renaming the column."""
        raise NotImplementedError

    def __str__(self) -> str:
        """Returns the column name.

        Important in the Jinja sandbox.
        """
        return self.column_name


class WhereFilterEntityFactory(ProtocolHint[QueryInterfaceEntityFactory]):
    """Creates a WhereFilterEntity.

    Each call to `create` adds an EntitySpec to entity_specs.
    """

    @override
    def _implements_protocol(self) -> QueryInterfaceEntityFactory:
        return self

    def __init__(  # noqa
        self,
        call_parameter_sets: FilterCallParameterSets,
        column_association_resolver: ColumnAssociationResolver,
    ):
        self._call_parameter_sets = call_parameter_sets
        self._column_association_resolver = column_association_resolver
        self.entity_specs: List[EntitySpec] = []

    def create(self, entity_name: str, entity_path: Sequence[str] = ()) -> WhereFilterEntity:
        """Create a WhereFilterEntity."""
        structured_name = DunderedNameFormatter.parse_name(entity_name)
        call_parameter_set = EntityCallParameterSet(
            entity_reference=EntityReference(element_name=entity_name),
            entity_path=(
                tuple(EntityReference(element_name=arg) for arg in entity_path) + structured_name.entity_links
            ),
        )
        assert call_parameter_set in self._call_parameter_sets.entity_call_parameter_sets

        entity_spec = self._convert_to_entity_spec(call_parameter_set)
        self.entity_specs.append(entity_spec)
        column_name = self._column_association_resolver.resolve_spec(entity_spec).column_name
        return WhereFilterEntity(column_name)

    def _convert_to_entity_spec(self, parameter_set: EntityCallParameterSet) -> EntitySpec:  # noqa: D
        return EntitySpec(
            element_name=parameter_set.entity_reference.element_name,
            entity_links=parameter_set.entity_path,
        )
