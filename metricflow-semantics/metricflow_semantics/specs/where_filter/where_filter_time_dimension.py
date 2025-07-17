from __future__ import annotations

from typing import Optional, Sequence, Tuple

from dbt_semantic_interfaces.call_parameter_sets import (
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dbt_semantic_interfaces.protocols.query_interface import (
    QueryInterfaceTimeDimensionFactory,
)
from dbt_semantic_interfaces.references import EntityReference, TimeDimensionReference
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from typing_extensions import override

from metricflow_semantics.errors.error_classes import InvalidQuerySyntax
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_spec_lookup import (
    FilterSpecResolutionLookUp,
    ResolvedSpecLookUpKey,
)
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.rendered_spec_tracker import RenderedSpecTracker
from metricflow_semantics.specs.where_filter.where_filter_dimension import WhereFilterDimension


class WhereFilterTimeDimension(WhereFilterDimension):
    """A time dimension that is passed in through the where filter parameter."""

    def __str__(self) -> str:
        """Returns the column name.

        Important in the Jinja sandbox.
        """
        call_parameter_set = TimeDimensionCallParameterSet(
            entity_path=self._entity_links,
            time_dimension_reference=TimeDimensionReference(self._element_name),
            time_granularity_name=self._time_granularity_name,
            date_part=self._date_part,
        )

        resolved_spec_key = ResolvedSpecLookUpKey(
            filter_location=self._where_filter_location,
            call_parameter_set=call_parameter_set,
        )

        resolved_spec = self._resolved_spec_lookup.checked_resolved_spec(resolved_spec_key)
        self._rendered_spec_tracker.record_rendered_spec(resolved_spec)
        column_association = self._column_association_resolver.resolve_spec(resolved_spec.spec)

        return column_association.column_name


class WhereFilterTimeDimensionFactory(ProtocolHint[QueryInterfaceTimeDimensionFactory]):
    """Creates a WhereFilterTimeDimension.

    Each call to `create` adds a TimeDimensionSpec to time_dimension_specs.
    """

    @override
    def _implements_protocol(self) -> QueryInterfaceTimeDimensionFactory:
        return self

    def __init__(  # noqa
        self,
        column_association_resolver: ColumnAssociationResolver,
        spec_resolution_lookup: FilterSpecResolutionLookUp,
        where_filter_location: WhereFilterLocation,
        rendered_spec_tracker: RenderedSpecTracker,
        custom_granularity_names: Tuple[str, ...],
    ):
        self._column_association_resolver = column_association_resolver
        self._resolved_spec_lookup = spec_resolution_lookup
        self._where_filter_location = where_filter_location
        self._rendered_spec_tracker = rendered_spec_tracker
        self._custom_granularity_names = custom_granularity_names

    def create(
        self,
        time_dimension_name: str,
        time_granularity_name: Optional[str] = None,
        entity_path: Sequence[str] = (),
        descending: Optional[bool] = None,
        date_part_name: Optional[str] = None,
    ) -> WhereFilterTimeDimension:
        """Create a WhereFilterTimeDimension."""
        if descending:
            raise InvalidQuerySyntax(
                "Can't set descending in the where clause. Try setting descending in the order_by clause instead"
            )

        time_granularity_name = time_granularity_name.lower() if time_granularity_name else None
        structured_name = StructuredLinkableSpecName.from_name(
            qualified_name=time_dimension_name.lower(), custom_granularity_names=self._custom_granularity_names
        )

        if (
            structured_name.time_granularity_name
            and time_granularity_name
            and structured_name.time_granularity_name != time_granularity_name
        ):
            raise InvalidQuerySyntax(
                f"Received different granularities in `time_dimension_name` parameter ('{time_dimension_name}') "
                f"and `time_granularity_name` parameter ('{time_granularity_name}')."
            )

        return WhereFilterTimeDimension(
            column_association_resolver=self._column_association_resolver,
            resolved_spec_lookup=self._resolved_spec_lookup,
            where_filter_location=self._where_filter_location,
            rendered_spec_tracker=self._rendered_spec_tracker,
            element_name=structured_name.element_name,
            entity_links=tuple(
                EntityReference(entity_link_name.lower())
                for entity_link_name in (tuple(entity_path) + structured_name.entity_link_names)
            ),
            time_granularity_name=time_granularity_name or structured_name.time_granularity_name,
            date_part=DatePart(date_part_name.lower()) if date_part_name else None,
        )
