from __future__ import annotations

from typing import List, Sequence

from dbt_semantic_interfaces.call_parameter_sets import FilterCallParameterSets, TimeDimensionCallParameterSet
from dbt_semantic_interfaces.naming.dundered import DunderedNameFormatter
from dbt_semantic_interfaces.references import EntityReference, TimeDimensionReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.query_interface import QueryInterfaceTimeDimension, QueryInterfaceTimeDimensionFactory
from metricflow.specs.specs import TimeDimensionSpec


class WhereFilterTimeDimension(QueryInterfaceTimeDimension):
    """A time dimension that is passed in through the where filter parameter."""

    def __init__(self, column_name: str):  # noqa
        self.column_name = column_name

    def __str__(self) -> str:
        """Returns the column name.

        Important in the Jinja sandbox.
        """
        return self.column_name


class WhereFilterTimeDimensionFactory(QueryInterfaceTimeDimensionFactory):
    """Creates a WhereFilterTimeDimension."""

    def __init__(  # noqa
        self,
        call_parameter_sets: FilterCallParameterSets,
        time_dimension_specs: List[TimeDimensionSpec],
        column_association_resolver: ColumnAssociationResolver,
    ):
        self._call_parameter_sets = call_parameter_sets
        self._time_dimension_specs = time_dimension_specs
        self._column_association_resolver = column_association_resolver

    def create(
        self, time_dimension_name: str, time_granularity_name: str, entity_path: Sequence[str] = ()
    ) -> WhereFilterTimeDimension:
        """Create a WhereFilterTimeDimension."""
        structured_name = DunderedNameFormatter.parse_name(time_dimension_name)
        call_parameter_set = TimeDimensionCallParameterSet(
            time_dimension_reference=TimeDimensionReference(element_name=structured_name.element_name),
            time_granularity=TimeGranularity(time_granularity_name),
            entity_path=(
                tuple(EntityReference(element_name=arg) for arg in entity_path) + structured_name.entity_links
            ),
        )
        assert call_parameter_set in self._call_parameter_sets.time_dimension_call_parameter_sets

        time_dimension_spec = self._convert_to_time_dimension_spec(call_parameter_set)
        self._time_dimension_specs.append(time_dimension_spec)
        column_names = self._column_association_resolver.resolve_spec(time_dimension_spec).column_name
        return WhereFilterTimeDimension(column_names)

    def _convert_to_time_dimension_spec(
        self,
        parameter_set: TimeDimensionCallParameterSet,
    ) -> TimeDimensionSpec:  # noqa: D
        return TimeDimensionSpec(
            element_name=parameter_set.time_dimension_reference.element_name,
            entity_links=parameter_set.entity_path,
            time_granularity=parameter_set.time_granularity,
        )
