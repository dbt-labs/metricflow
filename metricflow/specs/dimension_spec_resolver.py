from __future__ import annotations

from typing import Optional, Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    FilterCallParameterSets,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.naming.dundered import DunderedNameFormatter
from dbt_semantic_interfaces.references import DimensionReference, EntityReference, TimeDimensionReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.type_enums.date_part import DatePart

from metricflow.specs.specs import DEFAULT_TIME_GRANULARITY, DimensionSpec, TimeDimensionSpec


class DimensionSpecResolver:
    """Resolves specs for Dimension & TimeDimension given name, grain, & entity path. Utilized in where clause in Jinja syntax."""

    def __init__(self, call_parameter_sets: FilterCallParameterSets):  # noqa
        self._call_parameter_sets = call_parameter_sets

    def resolve_dimension_spec(self, name: str, entity_path: Sequence[str]) -> DimensionSpec:
        """Resolve Dimension spec with the call_parameter_sets."""
        structured_name = DunderedNameFormatter.parse_name(name)
        call_parameter_set = DimensionCallParameterSet(
            dimension_reference=DimensionReference(element_name=structured_name.element_name),
            entity_path=(
                tuple(EntityReference(element_name=arg) for arg in entity_path) + structured_name.entity_links
            ),
        )
        return DimensionSpec(
            element_name=call_parameter_set.dimension_reference.element_name,
            entity_links=call_parameter_set.entity_path,
        )

    def resolve_time_dimension_spec(
        self,
        name: str,
        time_granularity: Optional[TimeGranularity],
        entity_path: Sequence[str],
        date_part: Optional[DatePart],
    ) -> TimeDimensionSpec:
        """Resolve TimeDimension spec with the call_parameter_sets."""
        structured_name = DunderedNameFormatter.parse_name(name)
        call_parameter_set = TimeDimensionCallParameterSet(
            time_dimension_reference=TimeDimensionReference(element_name=structured_name.element_name),
            time_granularity=time_granularity,
            entity_path=(
                tuple(EntityReference(element_name=arg) for arg in entity_path) + structured_name.entity_links
            ),
            date_part=date_part,
        )
        assert call_parameter_set in self._call_parameter_sets.time_dimension_call_parameter_sets
        return TimeDimensionSpec(
            element_name=call_parameter_set.time_dimension_reference.element_name,
            entity_links=call_parameter_set.entity_path,
            time_granularity=(
                call_parameter_set.time_granularity
                # TODO: This should be updated once resolution of unspecified grain is supported.
                if call_parameter_set.time_granularity is not None
                else DEFAULT_TIME_GRANULARITY
            ),
            date_part=call_parameter_set.date_part,
        )
