from __future__ import annotations

from typing import List

from dbt_semantic_interfaces.call_parameter_sets import ParseWhereFilterException
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter

from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.protocols.query_parameter import GroupByParameter
from metricflow.query.query_exceptions import InvalidQueryException
from metricflow.specs.query_param_implementations import DimensionOrEntityParameter, TimeDimensionParameter


def parse_object_builder_naming_scheme(group_by_item_name: str) -> GroupByParameter:
    """Parses a string following the object-builder naming scheme into the corresponding GroupByParameter.

    The implementation of the query parameter classes seems incomplete and there needs to be follow up with the author
    of the query interface classes for the best approach. Right now, it seems like using the where filter is the only
    way to handle this conversion. However, it seems like this functionality should be abstracted into a module that
    handles operations related to the object-builder naming scheme. There is an additional issue where conversion
    from the element name / entity path to the name field in the query parameter objects requires going through
    StructuredLinkableSpecName.

    TODO: Replace this method once ideal implementations are in place.
    """
    try:
        call_parameter_sets = PydanticWhereFilter(
            where_sql_template="{{ " + group_by_item_name + " }}"
        ).call_parameter_sets
    except ParseWhereFilterException as e:
        raise InvalidQueryException(f"Error parsing `{group_by_item_name}`") from e

    group_by_parameters: List[GroupByParameter] = []

    for dimension_call_parameter_set in call_parameter_sets.dimension_call_parameter_sets:
        if len(dimension_call_parameter_set.entity_path) != 1:
            raise NotImplementedError(
                f"DimensionOrEntityParameter only supports a single item in the entity path. Got "
                f"{dimension_call_parameter_set} while handling `{group_by_item_name}`"
            )
        group_by_parameters.append(
            DimensionOrEntityParameter(
                name=StructuredLinkableSpecName(
                    element_name=dimension_call_parameter_set.dimension_reference.element_name,
                    entity_link_names=tuple(
                        entity_reference.element_name for entity_reference in dimension_call_parameter_set.entity_path
                    ),
                ).qualified_name
            )
        )

    for entity_call_parameter_set in call_parameter_sets.entity_call_parameter_sets:
        if len(entity_call_parameter_set.entity_path) != 1:
            raise NotImplementedError(
                f"DimensionOrEntityParameter only supports a single item in the entity path. Got "
                f"{entity_call_parameter_set} while handling `{group_by_item_name}`"
            )
        group_by_parameters.append(
            DimensionOrEntityParameter(
                name=StructuredLinkableSpecName(
                    element_name=entity_call_parameter_set.entity_reference.element_name,
                    entity_link_names=tuple(
                        entity_reference.element_name for entity_reference in entity_call_parameter_set.entity_path
                    ),
                ).qualified_name
            )
        )

    for time_dimension_parameter_set in call_parameter_sets.time_dimension_call_parameter_sets:
        group_by_parameters.append(
            TimeDimensionParameter(
                name=StructuredLinkableSpecName(
                    element_name=time_dimension_parameter_set.time_dimension_reference.element_name,
                    entity_link_names=tuple(
                        entity_reference.element_name for entity_reference in time_dimension_parameter_set.entity_path
                    ),
                ).qualified_name,
                grain=time_dimension_parameter_set.time_granularity,
            )
        )

    if len(group_by_parameters) != 1:
        raise InvalidQueryException(
            f"Did not get exactly 1 parameter while parsing `{group_by_item_name}`. Got: {group_by_parameters}"
        )

    return group_by_parameters[0]
