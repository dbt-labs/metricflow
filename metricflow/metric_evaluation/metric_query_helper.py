from __future__ import annotations

import itertools
import logging
from collections.abc import Iterable, Sequence
from typing import Optional

from dbt_semantic_interfaces.references import MetricReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantics.model.semantics.metric_lookup import MetricLookup
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.specs.non_additive_dimension_spec import NonAdditiveDimensionSpec
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.toolkit.collections.ordered_set import MutableOrderedSet, OrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple

from metricflow.dataflow.builder.simple_metric_input_spec_properties import SimpleMetricInputSpecProperties

logger = logging.getLogger(__name__)


class MetricQueryHelper:
    """Helper methods for metric queries.

    This is a WIP as additional consolidation / restructuring is needed.
    """

    def __init__(self, metric_lookup: MetricLookup) -> None:  # noqa: D107
        self._metric_lookup = metric_lookup

    def resolve_group_by_specs_for_time_offset_metric_input(
        self,
        queried_group_by_specs: Iterable[LinkableInstanceSpec],
        filter_specs: Iterable[WhereFilterSpec],
    ) -> OrderedSet[LinkableInstanceSpec]:
        """Helper to get the required group-by items for inputs to a time offset metric.

        The required group-by items may be a superset of the queried group-by items because:

        * A filter may contain group-by items that are not in the query, so we must generate a query that includes
        intersection of group-by items in the query and referenced in the filters, filter the result, then re-aggregate
        to only the group-by items in the query.

        * If there are any references to a time dimension at a custom time grain, we need to get the time dimension at
        the custom grain and then map it to the custom grain.
        """
        group_by_specs_referenced_in_filters: MutableOrderedSet[LinkableInstanceSpec] = MutableOrderedSet(
            itertools.chain.from_iterable(filter_spec.linkable_specs for filter_spec in filter_specs)
        )

        required_group_by_specs: MutableOrderedSet[LinkableInstanceSpec] = MutableOrderedSet(
            itertools.chain(
                queried_group_by_specs,
                group_by_specs_referenced_in_filters,
            )
        )
        required_time_dimension_specs_with_custom_grain = tuple(
            spec
            for spec in LinkableSpecSet.create_from_specs(required_group_by_specs).time_dimension_specs
            if spec.has_custom_grain
        )

        # Custom grains require joining to their base grain, so add base grain to extraneous specs.
        if len(required_time_dimension_specs_with_custom_grain) > 0:
            required_group_by_specs.update(
                spec.with_base_grain() for spec in required_time_dimension_specs_with_custom_grain
            )

        return required_group_by_specs

    def resolve_filter_application_for_time_offset_metric(  # noqa: D102
        self, metric_reference: MetricReference, filter_specs: Sequence[WhereFilterSpec]
    ) -> TimeOffsetFilterApplication:
        agg_time_dimension_specs: OrderedSet[
            LinkableInstanceSpec
        ] = self._metric_lookup.get_aggregation_time_dimension_specs(metric_reference)

        filters_on_agg_time_dimension_specs = []
        filters_not_on_agg_time_dimension_specs = []
        for filter_spec in filter_specs:
            if agg_time_dimension_specs.intersection(filter_spec.linkable_specs):
                filters_on_agg_time_dimension_specs.append(filter_spec)
            else:
                filters_not_on_agg_time_dimension_specs.append(filter_spec)

        return TimeOffsetFilterApplication(
            filters_before_time_spine_join=tuple(filters_not_on_agg_time_dimension_specs),
            filters_after_time_spine_join=tuple(filters_on_agg_time_dimension_specs),
        )

    def get_specs_for_non_additive_dimension(  # noqa: D102
        self, non_additive_dimension_spec: NonAdditiveDimensionSpec, non_additive_dimension_grain: TimeGranularity
    ) -> Sequence[LinkableInstanceSpec]:
        return non_additive_dimension_spec.linkable_specs(non_additive_dimension_grain)

    def __get_required_linkable_specs(
        self,
        queried_linkable_specs: LinkableSpecSet,
        filter_specs: Sequence[WhereFilterSpec],
        spec_properties: Optional[SimpleMetricInputSpecProperties] = None,
    ) -> LinkableSpecSet:
        """Get all required linkable specs for this query, including extraneous linkable specs.

        Extraneous linkable specs are specs that are used in this phase that should not show up in the final result
        unless it was already a requested spec in the query, e.g., a linkable spec used in where constraint is extraneous.
        """
        linkable_spec_sets_to_merge: list[LinkableSpecSet] = []
        for filter_spec in filter_specs:
            linkable_spec_sets_to_merge.append(LinkableSpecSet.create_from_specs(filter_spec.linkable_specs))

        if spec_properties is not None:
            non_additive_dimension_spec = spec_properties.non_additive_dimension_spec if spec_properties else None
            if non_additive_dimension_spec is not None:
                agg_time_dimension_grain = spec_properties.agg_time_dimension_grain
                linkable_spec_sets_to_merge.append(
                    LinkableSpecSet.create_from_specs(
                        non_additive_dimension_spec.linkable_specs(agg_time_dimension_grain)
                    )
                )

        extraneous_linkable_specs = LinkableSpecSet.merge_iterable(linkable_spec_sets_to_merge).dedupe()
        required_linkable_specs = queried_linkable_specs.merge(extraneous_linkable_specs).dedupe()

        # Custom grains require joining to their base grain, so add base grain to extraneous specs.
        if required_linkable_specs.time_dimension_specs_with_custom_grain:
            base_grain_set = LinkableSpecSet.create_from_specs(
                [spec.with_base_grain() for spec in required_linkable_specs.time_dimension_specs_with_custom_grain]
            )
            extraneous_linkable_specs = extraneous_linkable_specs.merge(base_grain_set).dedupe()
            required_linkable_specs = required_linkable_specs.merge(extraneous_linkable_specs).dedupe()

        return required_linkable_specs


@fast_frozen_dataclass()
class TimeOffsetFilterApplication:
    """Describes how filters should be applied."""

    filters_before_time_spine_join: AnyLengthTuple[WhereFilterSpec]
    filters_after_time_spine_join: AnyLengthTuple[WhereFilterSpec]
