from __future__ import annotations

import itertools
import logging
from collections.abc import Iterable

from metricflow_semantics.model.semantics.metric_lookup import MetricLookup
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.toolkit.collections.ordered_set import MutableOrderedSet, OrderedSet

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
