from __future__ import annotations

from typing import Sequence

from dbt_semantic_interfaces.call_parameter_sets import MetricCallParameterSet, TimeDimensionCallParameterSet
from dbt_semantic_interfaces.naming.dundered import StructuredDunderedName
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dbt_semantic_interfaces.protocols.query_interface import QueryInterfaceMetric, QueryInterfaceMetricFactory
from dbt_semantic_interfaces.references import (
    GroupByItemReference,
    MetricReference,
    TimeDimensionReference,
)
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
        group_by: Sequence[GroupByItemReference],
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
            group_by=tuple(self._group_by),
            metric_reference=MetricReference(self._element_name),
        )

        resolved_spec_key = ResolvedSpecLookUpKey(
            filter_location=self._where_filter_location,
            call_parameter_set=call_parameter_set,
        )

        resolved_spec = self._resolved_spec_lookup.checked_resolved_spec(resolved_spec_key)
        self._rendered_spec_tracker.record_rendered_spec(resolved_spec)
        column_association = self._column_association_resolver.resolve_spec(resolved_spec.spec)

        if any(group_by_ref.element_name == METRIC_TIME_ELEMENT_NAME for group_by_ref in self._group_by):
            metric_time_call_parameter_set = TimeDimensionCallParameterSet(
                entity_path=(),
                time_dimension_reference=TimeDimensionReference(METRIC_TIME_ELEMENT_NAME),
                time_granularity_name=None,
                date_part=None,
            )
            metric_time_spec_key = ResolvedSpecLookUpKey(
                filter_location=self._where_filter_location,
                call_parameter_set=metric_time_call_parameter_set,
            )
            if self._resolved_spec_lookup.spec_resolution_exists(metric_time_spec_key):
                metric_time_spec = self._resolved_spec_lookup.checked_resolved_spec(metric_time_spec_key)
                self._rendered_spec_tracker.record_rendered_spec(metric_time_spec)

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
        custom_granularity_names: Sequence[str],
    ) -> None:
        self._column_association_resolver = column_association_resolver
        self._resolved_spec_lookup = spec_resolution_lookup
        self._where_filter_location = where_filter_location
        self._rendered_spec_tracker = rendered_spec_tracker
        self._custom_granularity_names = tuple(custom_granularity_names)

    def create(self, metric_name: str, group_by: Sequence[str] = ()) -> WhereFilterMetric:
        """Create a WhereFilterMetric."""
        group_by_references = []
        for group_by_name in group_by:
            structured_name = StructuredDunderedName.parse_name(
                name=group_by_name.lower(), custom_granularity_names=self._custom_granularity_names
            )
            group_by_references.append(
                GroupByItemReference(
                    element_name=structured_name.element_name,
                    entity_links=structured_name.entity_links,
                    time_granularity_name=structured_name.time_granularity,
                )
            )
        return WhereFilterMetric(
            column_association_resolver=self._column_association_resolver,
            resolved_spec_lookup=self._resolved_spec_lookup,
            where_filter_location=self._where_filter_location,
            rendered_spec_tracker=self._rendered_spec_tracker,
            element_name=metric_name,
            group_by=tuple(group_by_references),
        )
