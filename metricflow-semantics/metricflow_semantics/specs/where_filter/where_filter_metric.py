from __future__ import annotations

from typing import Optional, Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    MetricCallParameterSet,
)
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
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
from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
from metricflow_semantics.specs.rendered_spec_tracker import RenderedSpecTracker
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec


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
        parent_time_dimension_spec: Optional[TimeDimensionSpec] = None,
    ) -> None:
        self._column_association_resolver = column_association_resolver
        self._resolved_spec_lookup = resolved_spec_lookup
        self._where_filter_location = where_filter_location
        self._rendered_spec_tracker = rendered_spec_tracker
        self._element_name = element_name
        self._group_by = tuple(group_by)
        self._parent_time_dimension_spec = parent_time_dimension_spec

    def descending(self, _is_descending: bool) -> QueryInterfaceMetric:
        """Set the sort order for order-by."""
        raise InvalidQuerySyntax(
            "Can't set descending in the where clause. Try setting descending in the order_by clause instead"
        )

    def __str__(self) -> str:
        """Returns the column name.

        Important in the Jinja sandbox.
        """
        # Check if metric_time is EXPLICITLY included in the group_by list from the YML definition
        # This is important because time granularity inheritance only happens when metric_time is
        # explicitly included in the filter's group_by list
        # Use a set for O(1) lookup instead of iterating through the list
        group_by_names = {group_by_ref.element_name.lower() for group_by_ref in self._group_by}
        has_metric_time = METRIC_TIME_ELEMENT_NAME.lower() in group_by_names

        # Create the call parameter set with the group_by items
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
        
        # Time granularity inheritance only happens when:
        # 1. 'metric_time' is EXPLICITLY included in the filter's group_by list
        # 2. We have a parent time dimension spec with a time granularity
        # 3. The resolved spec is a GroupByMetricSpec
        if (
            has_metric_time 
            and self._parent_time_dimension_spec is not None 
            and isinstance(resolved_spec, GroupByMetricSpec)
        ):
            # Create a new spec with the parent time granularity
            # The SQL rendering will use this column name which includes the time granularity
            column_association = self._column_association_resolver.resolve_spec(
                resolved_spec.with_time_granularity(self._parent_time_dimension_spec.time_granularity)
            )
        else:
            # If 'metric_time' is not in the group_by list, or we don't have a parent time dimension spec,
            # or the resolved spec is not a GroupByMetricSpec, then we don't apply time granularity inheritance
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
        parent_time_dimension_spec: Optional[TimeDimensionSpec] = None,
    ):
        self._column_association_resolver = column_association_resolver
        self._resolved_spec_lookup = spec_resolution_lookup
        self._where_filter_location = where_filter_location
        self._rendered_spec_tracker = rendered_spec_tracker
        self._parent_time_dimension_spec = parent_time_dimension_spec

    def create(self, metric_name: str, group_by: Sequence[str] = ()) -> WhereFilterMetric:
        """Create a WhereFilterMetric."""
        return WhereFilterMetric(
            column_association_resolver=self._column_association_resolver,
            resolved_spec_lookup=self._resolved_spec_lookup,
            where_filter_location=self._where_filter_location,
            rendered_spec_tracker=self._rendered_spec_tracker,
            element_name=metric_name,
            group_by=tuple(LinkableElementReference(group_by_name.lower()) for group_by_name in group_by),
            parent_time_dimension_spec=self._parent_time_dimension_spec,
        )
