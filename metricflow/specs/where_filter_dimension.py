from __future__ import annotations

import logging
from typing import Optional, Sequence, Union

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.naming.dundered import DunderedNameFormatter
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dbt_semantic_interfaces.protocols.query_interface import (
    QueryInterfaceDimension,
    QueryInterfaceDimensionFactory,
)
from dbt_semantic_interfaces.references import DimensionReference, EntityReference, TimeDimensionReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from typing_extensions import override

from metricflow.errors.errors import InvalidQuerySyntax
from metricflow.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow.query.group_by_item.filter_spec_resolution.filter_spec_lookup import (
    FilterSpecResolutionLookUp,
    ResolvedSpecLookUpKey,
)
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.rendered_spec_tracker import RenderedSpecTracker

logger = logging.getLogger(__name__)


class WhereFilterDimension(ProtocolHint[QueryInterfaceDimension]):
    """A dimension that is passed in through the where filter parameter."""

    @override
    def _implements_protocol(self) -> QueryInterfaceDimension:
        return self

    def __init__(  # noqa
        self,
        column_association_resolver: ColumnAssociationResolver,
        resolved_spec_lookup: FilterSpecResolutionLookUp,
        where_filter_location: WhereFilterLocation,
        rendered_spec_tracker: RenderedSpecTracker,
        element_name: str,
        entity_links: Sequence[EntityReference],
        time_grain: Optional[TimeGranularity] = None,
        date_part: Optional[DatePart] = None,
    ) -> None:
        self._column_association_resolver = column_association_resolver
        self._resolved_spec_lookup = resolved_spec_lookup
        self._where_filter_location = where_filter_location
        self._rendered_spec_tracker = rendered_spec_tracker
        self._element_name = element_name
        self._entity_links = tuple(entity_links)
        self._time_grain = time_grain
        self._date_part = date_part

    def grain(self, time_granularity_name: str) -> QueryInterfaceDimension:
        """The time granularity."""
        return WhereFilterDimension(
            column_association_resolver=self._column_association_resolver,
            resolved_spec_lookup=self._resolved_spec_lookup,
            where_filter_location=self._where_filter_location,
            rendered_spec_tracker=self._rendered_spec_tracker,
            element_name=self._element_name,
            entity_links=self._entity_links,
            time_grain=TimeGranularity(time_granularity_name.lower()),
            date_part=self._date_part,
        )

    def date_part(self, date_part_name: str) -> QueryInterfaceDimension:
        """The date_part requested to extract."""
        return WhereFilterDimension(
            column_association_resolver=self._column_association_resolver,
            resolved_spec_lookup=self._resolved_spec_lookup,
            where_filter_location=self._where_filter_location,
            rendered_spec_tracker=self._rendered_spec_tracker,
            element_name=self._element_name,
            entity_links=self._entity_links,
            time_grain=self._time_grain,
            date_part=DatePart(date_part_name.lower()),
        )

    def descending(self, _is_descending: bool) -> QueryInterfaceDimension:
        """Set the sort order for order-by."""
        raise InvalidQuerySyntax("descending is invalid in the where parameter")

    def __str__(self) -> str:
        """Returns the column name.

        Important in the Jinja sandbox.
        """
        call_parameter_set: Union[TimeDimensionCallParameterSet, DimensionCallParameterSet]
        if self._time_grain is not None or self._date_part is not None:
            call_parameter_set = TimeDimensionCallParameterSet(
                entity_path=self._entity_links,
                time_dimension_reference=TimeDimensionReference(self._element_name),
                time_granularity=self._time_grain,
                date_part=self._date_part,
            )
        else:
            call_parameter_set = DimensionCallParameterSet(
                entity_path=self._entity_links,
                dimension_reference=DimensionReference(self._element_name),
            )

        resolved_spec = self._resolved_spec_lookup.checked_resolved_spec(
            ResolvedSpecLookUpKey(
                filter_location=self._where_filter_location,
                call_parameter_set=call_parameter_set,
            )
        )
        self._rendered_spec_tracker.record_rendered_spec(resolved_spec)
        column_association = self._column_association_resolver.resolve_spec(resolved_spec)

        return column_association.column_name


class WhereFilterDimensionFactory(ProtocolHint[QueryInterfaceDimensionFactory]):
    """Creates a WhereFilterDimension.

    Each call to `create` adds a WhereFilterDimension to created.
    """

    @override
    def _implements_protocol(self) -> QueryInterfaceDimensionFactory:
        return self

    def __init__(  # noqa
        self,
        column_association_resolver: ColumnAssociationResolver,
        spec_resolution_lookup: FilterSpecResolutionLookUp,
        where_filter_location: WhereFilterLocation,
        rendered_spec_tracker: RenderedSpecTracker,
    ):
        self._column_association_resolver = column_association_resolver
        self._resolved_spec_lookup = spec_resolution_lookup
        self._where_filter_location = where_filter_location
        self._rendered_spec_tracker = rendered_spec_tracker

    def create(self, name: str, entity_path: Sequence[str] = ()) -> WhereFilterDimension:
        """Create a WhereFilterDimension."""
        structured_name = DunderedNameFormatter.parse_name(name.lower())

        return WhereFilterDimension(
            column_association_resolver=self._column_association_resolver,
            resolved_spec_lookup=self._resolved_spec_lookup,
            where_filter_location=self._where_filter_location,
            rendered_spec_tracker=self._rendered_spec_tracker,
            element_name=structured_name.element_name,
            entity_links=tuple(EntityReference(entity_link_name.lower()) for entity_link_name in entity_path)
            + structured_name.entity_links,
        )
