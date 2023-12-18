from __future__ import annotations

from typing import Optional, Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    EntityCallParameterSet,
)
from dbt_semantic_interfaces.naming.dundered import DunderedNameFormatter
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dbt_semantic_interfaces.protocols.query_interface import QueryInterfaceEntity, QueryInterfaceEntityFactory
from dbt_semantic_interfaces.references import EntityReference
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


class WhereFilterEntity(ProtocolHint[QueryInterfaceEntity]):
    """An entity that is passed in through the where filter parameter."""

    @override
    def _implements_protocol(self) -> QueryInterfaceEntity:
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

    def descending(self, _is_descending: bool) -> QueryInterfaceEntity:
        """Set the sort order for order-by."""
        raise InvalidQuerySyntax(
            "Can't set descending in the where clause. Try setting descending in the order_by clause instead"
        )

    def __str__(self) -> str:
        """Returns the column name.

        Important in the Jinja sandbox.
        """
        call_parameter_set = EntityCallParameterSet(
            entity_path=self._entity_links,
            entity_reference=EntityReference(self._element_name),
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


class WhereFilterEntityFactory(ProtocolHint[QueryInterfaceEntityFactory]):
    """Creates a WhereFilterEntity.

    Each call to `create` adds an EntitySpec to entity_specs.
    """

    @override
    def _implements_protocol(self) -> QueryInterfaceEntityFactory:
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

    def create(self, entity_name: str, entity_path: Sequence[str] = ()) -> WhereFilterEntity:
        """Create a WhereFilterEntity."""
        structured_name = DunderedNameFormatter.parse_name(entity_name.lower())

        return WhereFilterEntity(
            column_association_resolver=self._column_association_resolver,
            resolved_spec_lookup=self._resolved_spec_lookup,
            where_filter_location=self._where_filter_location,
            rendered_spec_tracker=self._rendered_spec_tracker,
            element_name=structured_name.element_name,
            entity_links=tuple(EntityReference(entity_link_name.lower()) for entity_link_name in entity_path)
            + structured_name.entity_links,
        )
