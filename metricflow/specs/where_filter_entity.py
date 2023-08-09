from typing import List
from metricflow.specs.query_interface import QueryInterfaceEntityFactory, QueryInterfaceEntity

from typing import Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    EntityCallParameterSet,
)
from dbt_semantic_interfaces.naming.dundered import DunderedNameFormatter

from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.specs import EntitySpec
from dbt_semantic_interfaces.call_parameter_sets import FilterCallParameterSets
from dbt_semantic_interfaces.references import EntityReference


class WhereFilterEntity(QueryInterfaceEntity):
    def __init__(self, column_name: str):
        self.column_name = column_name

    def __str__(self):
        return self.column_name


class WhereFilterEntityFactory(QueryInterfaceEntityFactory):
    def __init__(
        self,
        call_parameter_sets: FilterCallParameterSets,
        entity_specs: List[EntitySpec],
        column_association_resolver: ColumnAssociationResolver,
    ):
        self._call_parameter_sets = call_parameter_sets
        self._entity_specs = entity_specs
        self._column_association_resolver = column_association_resolver

    def create(self, entity_name: str, entity_path: Sequence[str] = ()) -> WhereFilterEntity:
        """Gets called by Jinja when rendering {{ Entity(...) }}."""
        structured_name = DunderedNameFormatter.parse_name(entity_name)
        call_parameter_set = EntityCallParameterSet(
            entity_reference=EntityReference(element_name=entity_name),
            entity_path=(
                tuple(EntityReference(element_name=arg) for arg in entity_path) + structured_name.entity_links
            ),
        )
        assert call_parameter_set in self._call_parameter_sets.entity_call_parameter_sets

        entity_spec = self._convert_to_entity_spec(call_parameter_set)
        self._entity_specs.append(entity_spec)
        column_name = self._column_association_resolver.resolve_spec(entity_spec).column_name
        return column_name

    def _convert_to_entity_spec(self, parameter_set: EntityCallParameterSet) -> EntitySpec:  # noqa: D
        return EntitySpec(
            element_name=parameter_set.entity_reference.element_name,
            entity_links=parameter_set.entity_path,
        )
