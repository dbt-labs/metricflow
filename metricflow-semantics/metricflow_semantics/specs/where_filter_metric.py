from __future__ import annotations

from typing import Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    MetricCallParameterSet,
)
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dbt_semantic_interfaces.protocols.query_interface import QueryInterfaceMetric, QueryInterfaceMetricFactory
from dbt_semantic_interfaces.references import EntityReference, LinkableElementReference, MetricReference
from typing_extensions import override

from metricflow_semantics.errors.error_classes import InvalidQuerySyntax
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_spec_lookup import (
    FilterSpecResolutionLookUp,
    ResolvedSpecLookUpKey,
)
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.rendered_spec_tracker import RenderedSpecTracker


class WhereFilterMetric(ProtocolHint[QueryInterfaceMetric]):
    """A metric that is passed in through the where filter parameter."""

    @override
    def _implements_protocol(self) -> QueryInterfaceMetric:
        return self

    def __init__(  # noqa
        self,
        column_association_resolver: ColumnAssociationResolver,
        resolved_spec_lookup: FilterSpecResolutionLookUp,
        where_filter_location: WhereFilterLocation,
        rendered_spec_tracker: RenderedSpecTracker,
        element_name: str,
        group_by: Sequence[LinkableElementReference],
    ) -> None:
        self._column_association_resolver = column_association_resolver
        self._resolved_spec_lookup = resolved_spec_lookup
        self._where_filter_location = where_filter_location
        self._rendered_spec_tracker = rendered_spec_tracker
        self._element_name = element_name
        self._group_by = tuple(group_by)

    def descending(self, _is_descending: bool) -> QueryInterfaceMetric:
        """Set the sort order for order-by."""
        raise InvalidQuerySyntax(
            "Can't set descending in the where clause. Try setting descending in the order_by clause instead"
        )

    def __str__(self) -> str:
        """Returns the column name.

        Important in the Jinja sandbox.
        """
        call_parameter_set = MetricCallParameterSet(
            group_by=tuple(EntityReference(element_name=group_by_ref.element_name) for group_by_ref in self._group_by),
            metric_reference=MetricReference(self._element_name),
        )

        resolved_spec_key = ResolvedSpecLookUpKey(
            filter_location=self._where_filter_location,
            call_parameter_set=call_parameter_set,
        )

        resolved_spec = self._resolved_spec_lookup.checked_resolved_spec(resolved_spec_key)
        resolved_elements = self._resolved_spec_lookup.checked_resolved_linkable_elements(resolved_spec_key)
        self._rendered_spec_tracker.record_rendered_spec_to_elements_mapping((resolved_spec, resolved_elements))
        column_association = self._column_association_resolver.resolve_spec(resolved_spec)

        return column_association.column_name


class WhereFilterMetricFactory(ProtocolHint[QueryInterfaceMetricFactory]):
    """Creates a WhereFilterMetric.

    Each call to `create` adds a MetricSpec to metric_specs.
    """

    @override
    def _implements_protocol(self) -> QueryInterfaceMetricFactory:
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

    def create(self, metric_name: str, group_by: Sequence[str] = ()) -> WhereFilterMetric:
        """Create a WhereFilterMetric."""
        return WhereFilterMetric(
            column_association_resolver=self._column_association_resolver,
            resolved_spec_lookup=self._resolved_spec_lookup,
            where_filter_location=self._where_filter_location,
            rendered_spec_tracker=self._rendered_spec_tracker,
            element_name=metric_name,
            group_by=tuple(LinkableElementReference(group_by_name.lower()) for group_by_name in group_by),
        )
