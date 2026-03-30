from __future__ import annotations

import itertools
import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from typing import Optional

from metricflow_semantics.errors.custom_grain_not_supported import error_if_not_standard_grain
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.time_window import TimeWindow
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.specs.where_filter.where_filter_spec_factory import WhereFilterSpecFactory
from metricflow_semantics.toolkit.collections.ordered_set import MutableOrderedSet, OrderedSet

from metricflow.metric_evaluation.plan.me_plan import (
    MetricEvaluationPlan,
)
from metricflow.plan_conversion.node_processor import PredicatePushdownState
from metricflow_semantic_interfaces.references import MetricReference
from metricflow_semantic_interfaces.type_enums import TimeGranularity

logger = logging.getLogger(__name__)


class MetricEvaluationPlanner(ABC):
    """ABC for a planner that creates a metric evaluation plan."""

    def __init__(  # noqa: D107
        self, manifest_object_lookup: ManifestObjectLookup, column_association_resolver: ColumnAssociationResolver
    ) -> None:
        self._manifest_object_lookup = manifest_object_lookup
        self._column_association_resolver = column_association_resolver

    @abstractmethod
    def build_plan(
        self,
        metric_specs: Sequence[MetricSpec],
        group_by_item_specs: Sequence[LinkableInstanceSpec],
        predicate_pushdown_state: PredicatePushdownState,
        filter_spec_factory: WhereFilterSpecFactory,
    ) -> MetricEvaluationPlan:
        """Builds a plan for the given metrics / group-by items."""
        raise NotImplementedError

    def _required_group_by_items_for_inputs_to_a_time_offset_metric(
        self,
        queried_group_by_specs: Iterable[LinkableInstanceSpec],
        filter_specs: Iterable[WhereFilterSpec],
    ) -> OrderedSet[LinkableInstanceSpec]:
        """Helper to get the required group-by items for a query.

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

    def _build_input_metric_specs_for_derived_metric(
        self,
        metric_name: str,
        additional_filter_specs: Iterable[WhereFilterSpec],
        filter_spec_factory: WhereFilterSpecFactory,
    ) -> Sequence[MetricSpec]:
        """For a metric that has input metrics (e.g. derived), return the metric specs for the input."""
        metric = self._manifest_object_lookup.get_metric(metric_name)

        # This is the filter that's defined for the metric in the configs.
        metric_definition_filter_specs = filter_spec_factory.create_from_where_filter_intersection(
            filter_location=WhereFilterLocation.for_metric(MetricReference(element_name=metric_name)),
            filter_intersection=metric.filter,
        )

        input_metric_specs: list[MetricSpec] = []

        for input_metric in metric.input_metrics:
            where_filter_specs = list(
                filter_spec_factory.create_from_where_filter_intersection(
                    filter_location=WhereFilterLocation.for_input_metric(
                        input_metric_reference=input_metric.as_reference
                    ),
                    filter_intersection=input_metric.filter,
                )
            )
            where_filter_specs.extend(metric_definition_filter_specs)
            where_filter_specs.extend(additional_filter_specs)

            input_metric_offset_to_grain: Optional[TimeGranularity] = None
            if input_metric.offset_to_grain is not None:
                input_metric_offset_to_grain = error_if_not_standard_grain(
                    context=f"Metric({metric.name}).InputMetric({input_metric.name}).offset_to_grain",
                    input_granularity=input_metric.offset_to_grain,
                )

            spec = MetricSpec.create(
                element_name=input_metric.name,
                where_filter_specs=where_filter_specs,
                alias=input_metric.alias,
                offset_window=(
                    TimeWindow(
                        count=input_metric.offset_window.count,
                        granularity=input_metric.offset_window.granularity,
                    )
                    if input_metric.offset_window
                    else None
                ),
                offset_to_grain=input_metric_offset_to_grain,
            )
            input_metric_specs.append(spec)
        return input_metric_specs
