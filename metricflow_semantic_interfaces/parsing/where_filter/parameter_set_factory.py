from __future__ import annotations

from enum import Enum
from typing import Optional, Sequence

from metricflow_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    MetricCallParameterSet,
    ParseJinjaObjectException,
    TimeDimensionCallParameterSet,
)
from metricflow_semantic_interfaces.naming.dundered import StructuredDunderedName
from metricflow_semantic_interfaces.naming.keywords import is_metric_time_name
from metricflow_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    LinkableElementReference,
    MetricReference,
    TimeDimensionReference,
)
from metricflow_semantic_interfaces.type_enums.date_part import DatePart


class QueryItemLocation(Enum):
    """The location of the input string in the query."""

    ORDER_BY = "order_by"
    NON_ORDER_BY = "non_order_by"


class ParameterSetFactory:
    """Creates parameter sets for use in the Jinja sandbox.

    This class does the following:
      1. Parses element references (e.g., "{{ Dimension('listing__is_lux') }}") out of where filter expressions
      2. Extracts reference attributes (e.g., grain for "{{ TimeDimension('metric_time', grain='martian_year') }}")
      3. Allows use of standard time granularities in name strings, (e.g., "{{ Dimension('metric_time__year') }}")

    This class does not do direct validation of any custom granularity values, nor does it allow for use of custom
    granularities as parts of element reference names. So we can parse "{{ Dimension('shuttle__launch_time__year') }}"
    into a valid TimeDimension object with yearly granularity, but we will not correctly parse something like
    "{{Dimension('shuttle__launch_time__martian_year')}}" - this will return a dimension named `martian_year` with the
    entity link path of ['shuttle', 'launch_time']. Since custom granularity names will not be allowed to be re-used
    as dimension names this will fail to match anything defined in the semantic manifest, but the error management
    experience will not be as clean and direct as it was for standard granularities.
    """

    @staticmethod
    def _exception_message_for_incorrect_format(element_name: str) -> str:
        return (
            f"Name is in an incorrect format: {repr(element_name)}. It should be of the form: "
            f"<primary entity name>__<dimension_name>"
        )

    @staticmethod
    def create_time_dimension(
        time_dimension_name: str,
        custom_granularity_names: Sequence[str],
        time_granularity_name: Optional[str] = None,
        entity_path: Sequence[str] = (),
        date_part_name: Optional[str] = None,
        descending: Optional[bool] = None,
    ) -> TimeDimensionCallParameterSet:
        """Gets called by Jinja when rendering {{ TimeDimension(...) }}.

        There is a lot of strangeness around the time granularity specification here. Historically,
        we accepted time dimension names of the form `metric_time__week` or `customer__registration_date__month`
        in this interface. We have not yet fully deprecated this, and it's unclear if we ever will.

        Key points to note:
          1. The time dimension name parsing only accepts standard time granularities. This will not change.
          2. The time granularity parameter is what we want everybody to use because it's more explicit.
          3. The time granularity parameter will support custom granularities, so that's nice

        While this all may seem pretty bad it's not as terrible as all that - this class is only used
        for parsing where filters. When we solve the problems with our current where filter spec this will
        persist as a backwards compatibility model, but nothing more.
        """
        group_by_item_name = StructuredDunderedName.parse_name(
            name=time_dimension_name, custom_granularity_names=custom_granularity_names
        )
        if len(group_by_item_name.entity_links) != 1 and not is_metric_time_name(group_by_item_name.element_name):
            raise ParseJinjaObjectException(
                ParameterSetFactory._exception_message_for_incorrect_format(time_dimension_name)
            )
        grain_parsed_from_name = group_by_item_name.time_granularity
        inputs_are_mismatched = (
            grain_parsed_from_name is not None
            and time_granularity_name is not None
            and time_granularity_name != grain_parsed_from_name
        )

        if inputs_are_mismatched:
            raise ParseJinjaObjectException(
                f"Received different grains in `time_dimension_name` parameter ('{time_dimension_name}') "
                f"and `time_granularity_name` parameter ('{time_granularity_name}'). Remove the grain suffix "
                f"(`{grain_parsed_from_name}`) from the time dimension name and use the `time_granularity_name` "
                "parameter to specify the intendend grain."
            )

        time_granularity_name = grain_parsed_from_name or time_granularity_name

        return TimeDimensionCallParameterSet(
            time_dimension_reference=TimeDimensionReference(element_name=group_by_item_name.element_name),
            entity_path=(
                tuple(EntityReference(element_name=arg) for arg in entity_path) + group_by_item_name.entity_links
            ),
            time_granularity_name=time_granularity_name.lower() if time_granularity_name else None,
            date_part=DatePart(date_part_name.lower()) if date_part_name else None,
            descending=descending,
        )

    @staticmethod
    def create_dimension(
        dimension_name: str, entity_path: Sequence[str] = (), descending: Optional[bool] = None
    ) -> DimensionCallParameterSet:
        """Gets called by Jinja when rendering {{ Dimension(...) }}."""
        group_by_item_name = StructuredDunderedName.parse_name(name=dimension_name, custom_granularity_names=())

        if len(group_by_item_name.entity_links) != 1 and not is_metric_time_name(group_by_item_name.element_name):
            raise ParseJinjaObjectException(ParameterSetFactory._exception_message_for_incorrect_format(dimension_name))

        return DimensionCallParameterSet(
            dimension_reference=DimensionReference(element_name=group_by_item_name.element_name),
            entity_path=(
                tuple(EntityReference(element_name=arg) for arg in entity_path) + group_by_item_name.entity_links
            ),
            descending=descending,
        )

    @staticmethod
    def create_entity(
        entity_name: str, entity_path: Sequence[str] = (), descending: Optional[bool] = None
    ) -> EntityCallParameterSet:
        """Gets called by Jinja when rendering {{ Entity(...) }}."""
        structured_dundered_name = StructuredDunderedName.parse_name(name=entity_name, custom_granularity_names=())
        if structured_dundered_name.time_granularity is not None:
            raise ParseJinjaObjectException(
                f"Name is in an incorrect format: {repr(entity_name)}. " f"It should not contain a time grain suffix."
            )

        additional_entity_path_elements = tuple(
            EntityReference(element_name=entity_path_item) for entity_path_item in entity_path
        )

        return EntityCallParameterSet(
            entity_path=additional_entity_path_elements + structured_dundered_name.entity_links,
            entity_reference=EntityReference(element_name=structured_dundered_name.element_name),
            descending=descending,
        )

    @staticmethod
    def create_metric(
        metric_name: str,
        group_by: Sequence[str] = (),
        query_item_location: QueryItemLocation = QueryItemLocation.NON_ORDER_BY,
        descending: Optional[bool] = None,
    ) -> MetricCallParameterSet:
        """Gets called by Jinja when rendering {{ Metric(...) }}."""
        # Metric(...) syntax is required in saved_query.order_by to apply descending. Don't require group by there.
        if query_item_location == QueryItemLocation.NON_ORDER_BY and not group_by:
            raise ParseJinjaObjectException(
                "`group_by` parameter is required for Metric in where filter. This is needed to determine 1) the "
                "granularity to aggregate the metric to and 2) how to join the metric to the rest of the query."
            )
        return MetricCallParameterSet(
            metric_reference=MetricReference(element_name=metric_name),
            group_by=tuple([LinkableElementReference(element_name=group_by_name) for group_by_name in group_by]),
            descending=descending,
        )
