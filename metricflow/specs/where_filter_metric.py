from __future__ import annotations

from typing import Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    MetricCallParameterSet,
)
from dbt_semantic_interfaces.parsing.where_filter.where_filter_entity import WhereFilterMetric, WhereFilterMetricFactory
from dbt_semantic_interfaces.references import EntityReference

from metricflow.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow.query.group_by_item.filter_spec_resolution.filter_spec_lookup import (
    FilterSpecResolutionLookUp,
    ResolvedSpecLookUpKey,
)
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.rendered_spec_tracker import RenderedSpecTracker


class WhereFilterMetric:
    """A metric that is passed in through the where filter parameter."""

    def __init__(  # noqa
        self,
        column_association_resolver: ColumnAssociationResolver,
        resolved_spec_lookup: FilterSpecResolutionLookUp,
        where_filter_location: WhereFilterLocation,
        rendered_spec_tracker: RenderedSpecTracker,
        metric_call_parameter_set: MetricCallParameterSet,
    ) -> None:
        self._column_association_resolver = column_association_resolver
        self._resolved_spec_lookup = resolved_spec_lookup
        self._where_filter_location = where_filter_location
        self._rendered_spec_tracker = rendered_spec_tracker
        self._metric_call_parameter_set = metric_call_parameter_set

    def __str__(self) -> str:
        """Returns the column name associated with the where filter metric. Important in the Jinja sandbox.

        Along the way: parses call params into a LinkableInstanceSpec and checks that the spec is valid to use
        in the where filter for this query. Also stores spec for future use.
        """
        # Temp: parse group by from LinkableElementReferences to EntityReferences.
        # Currently we only support entities here, but later will add support for dimensions too.
        updated_call_parameter_set = MetricCallParameterSet(
            group_by=tuple(
                EntityReference(element_name=group_by_ref.element_name)
                for group_by_ref in self._metric_call_parameter_set.group_by
            ),
            metric_reference=self._metric_call_parameter_set.metric_reference,
        )
        resolved_spec = self._resolved_spec_lookup.checked_resolved_spec(
            ResolvedSpecLookUpKey(
                filter_location=self._where_filter_location,
                call_parameter_set=updated_call_parameter_set,
            )
        )
        self._rendered_spec_tracker.record_rendered_spec(resolved_spec)
        column_association = self._column_association_resolver.resolve_spec(resolved_spec)

        return column_association.column_name


class MFWhereFilterMetricFactory(WhereFilterMetricFactory):
    """Creates a WhereFilterMetric.

    Each call to `create` adds a MetricSpec to metric_specs.
    """

    def __init__(  # noqa
        self,
        column_association_resolver: ColumnAssociationResolver,
        spec_resolution_lookup: FilterSpecResolutionLookUp,
        where_filter_location: WhereFilterLocation,
        rendered_spec_tracker: RenderedSpecTracker,
    ):
        super().__init__()
        self._column_association_resolver = column_association_resolver
        self._resolved_spec_lookup = spec_resolution_lookup
        self._where_filter_location = where_filter_location
        self._rendered_spec_tracker = rendered_spec_tracker

    def create(self, metric_name: str, group_by: Sequence[str] = ()) -> WhereFilterMetric:
        """Create a WhereFilterMetric."""
        # Parent method builds metric_call_parameter_sets & stores on class for future use.
        super().create(metric_name=metric_name, group_by=group_by)

        assert (
            len(self.metric_call_parameter_sets) > 0
        ), f"Expected at least one MetricCallParameterSet when parsing Jinja Metric. Got: {self.metric_call_parameter_sets}"
        return WhereFilterMetric(
            column_association_resolver=self._column_association_resolver,
            resolved_spec_lookup=self._resolved_spec_lookup,
            where_filter_location=self._where_filter_location,
            rendered_spec_tracker=self._rendered_spec_tracker,
            # This metric is the latest one that's been parsed, so it's at the end of the list.
            metric_call_parameter_set=self.metric_call_parameter_sets[-1],
        )
