from __future__ import annotations

import logging
from typing import Dict, Final, Iterable, Optional, Sequence, Set

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols.metric import Metric, MetricInputMeasure, MetricType
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.references import MeasureReference, MetricReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow_semantics.errors.error_classes import DuplicateMetricError, MetricNotFoundError, NonExistentMeasureError
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.annotated_spec_linkable_element_set import (
    GroupByItemSet,
)
from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseGroupByItemSet
from metricflow_semantics.model.semantics.linkable_spec_resolver import (
    GroupByItemSetResolver,
)
from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

logger = logging.getLogger(__name__)


DEFAULT_COMMON_SET_FILTER: Final[GroupByItemSetFilter] = GroupByItemSetFilter.create(
    any_properties_denylist=(GroupByItemProperty.METRIC,)
)


class MetricLookup:
    """Tracks semantic information for metrics by linking them to semantic models."""

    def __init__(
        self,
        semantic_manifest: SemanticManifest,
        semantic_model_lookup: SemanticModelLookup,
        custom_granularities: Dict[str, ExpandedTimeGranularity],
        group_by_item_set_resolver: GroupByItemSetResolver,
    ) -> None:
        """Initializer.

        Args:
            semantic_manifest: used to fetch and load the metrics and initialize the linkable spec resolver
            semantic_model_lookup: provides access to semantic model metadata for various lookup operations
        """
        self._metrics: Dict[MetricReference, Metric] = {}
        self._semantic_model_lookup = semantic_model_lookup
        self._custom_granularities = custom_granularities

        for metric in semantic_manifest.metrics:
            self._add_metric(metric)

        self._group_by_item_set_resolver = group_by_item_set_resolver

        # Cache for `get_min_queryable_time_granularity()`
        self._metric_reference_to_min_metric_time_grain: Dict[MetricReference, TimeGranularity] = {}

        # Cache for `get_valid_agg_time_dimensions_for_metric()`.
        self._metric_reference_to_valid_agg_time_dimension_specs: Dict[
            MetricReference, Sequence[TimeDimensionSpec]
        ] = {}

    def get_group_by_items_for_distinct_values_query(
        self, set_filter: GroupByItemSetFilter = GroupByItemSetFilter.create()
    ) -> BaseGroupByItemSet:
        """Return the reachable linkable elements for a dimension values query with no metrics."""
        return self._group_by_item_set_resolver.get_set_for_distinct_values_query(set_filter)

    def get_common_group_by_items(
        self,
        measure_references: Iterable[MeasureReference] = (),
        metric_references: Iterable[MetricReference] = (),
        set_filter: GroupByItemSetFilter = DEFAULT_COMMON_SET_FILTER,
    ) -> BaseGroupByItemSet:
        """Gets the set of the valid group-by items common to all inputs."""
        if set_filter.element_name_allowlist is None:
            return self._group_by_item_set_resolver.get_common_set(
                measure_references=measure_references,
                metric_references=metric_references,
                set_filter=set_filter,
            )

        # If the filter specifies element names, make the call to the resolver without element names to get better
        # cache hit rates.
        filter_without_element_name_condition = set_filter.without_element_name_allowlist()

        result_superset = self._group_by_item_set_resolver.get_common_set(
            measure_references=measure_references,
            metric_references=metric_references,
            set_filter=filter_without_element_name_condition,
        )

        return GroupByItemSet(
            annotated_specs=tuple(
                annotated_spec
                for annotated_spec in result_superset.annotated_specs
                if annotated_spec.element_name in set_filter.element_name_allowlist
            )
        )

    def get_metrics(self, metric_references: Iterable[MetricReference]) -> Sequence[Metric]:  # noqa: D102
        res = []
        for metric_reference in metric_references:
            if metric_reference not in self._metrics:
                raise MetricNotFoundError(
                    f"Unable to find metric `{metric_reference}`. Perhaps it has not been registered"
                )
            res.append(self._metrics[metric_reference])

        return res

    @property
    def metric_references(self) -> FrozenOrderedSet[MetricReference]:  # noqa: D102
        return FrozenOrderedSet(sorted(self._metrics.keys()))

    def get_metric(self, metric_reference: MetricReference) -> Metric:  # noqa: D102
        if metric_reference not in self._metrics:
            raise MetricNotFoundError(f"Unable to find metric `{metric_reference}`. Perhaps it has not been registered")
        return self._metrics[metric_reference]

    def _add_metric(self, metric: Metric) -> None:
        """Add metric, validating presence of required measures."""
        metric_reference = MetricReference(element_name=metric.name)
        if metric_reference in self._metrics:
            raise DuplicateMetricError(f"Metric `{metric.name}` has already been registered")
        for measure_reference in metric.measure_references:
            if measure_reference not in self._semantic_model_lookup.measure_lookup.measure_references:
                raise NonExistentMeasureError(
                    f"Metric `{metric.name}` references measure `{measure_reference}` which has not been registered"
                )
        self._metrics[metric_reference] = metric

    def configured_input_measure_for_metric(  # noqa: D102
        self, metric_reference: MetricReference
    ) -> Optional[MetricInputMeasure]:  # noqa: D102
        metric = self.get_metric(metric_reference=metric_reference)
        if metric.type is MetricType.CUMULATIVE or metric.type is MetricType.SIMPLE:
            assert len(metric.input_measures) == 1, "Simple and cumulative metrics should have one input measure."
            return metric.input_measures[0]
        elif (
            metric.type is MetricType.RATIO or metric.type is MetricType.DERIVED or metric.type is MetricType.CONVERSION
        ):
            return None
        else:
            assert_values_exhausted(metric.type)

    def contains_cumulative_or_time_offset_metric(self, metric_references: Sequence[MetricReference]) -> bool:
        """Returns true if any of the specs correspond to a cumulative metric or a derived metric with time offset."""
        for metric_reference in metric_references:
            metric = self.get_metric(metric_reference)
            if metric.type == MetricType.CUMULATIVE:
                return True
            elif metric.type == MetricType.DERIVED:
                for input_metric in metric.type_params.metrics or []:
                    if input_metric.offset_window or input_metric.offset_to_grain:
                        return True
        return False

    def _get_agg_time_dimension_specs_for_metric(
        self, metric_reference: MetricReference
    ) -> Sequence[TimeDimensionSpec]:
        """Retrieves the aggregate time dimensions associated with the metric's measures."""
        metric = self.get_metric(metric_reference)
        specs: Set[TimeDimensionSpec] = set()
        for input_measure in metric.input_measures:
            measure_properties = self._semantic_model_lookup.measure_lookup.get_properties(
                measure_reference=input_measure.measure_reference
            )
            specs.update(measure_properties.agg_time_dimension_specs)
        return list(specs)

    def get_valid_agg_time_dimensions_for_metric(
        self, metric_reference: MetricReference
    ) -> Sequence[TimeDimensionSpec]:
        """Get the agg time dimension specs that can be used in place of metric time for this metric, if applicable."""
        result = self._metric_reference_to_valid_agg_time_dimension_specs.get(metric_reference)
        if result is not None:
            return result

        result = self._get_valid_agg_time_dimensions_for_metric(metric_reference)
        self._metric_reference_to_valid_agg_time_dimension_specs[metric_reference] = result

        return result

    def _get_valid_agg_time_dimensions_for_metric(
        self, metric_reference: MetricReference
    ) -> Sequence[TimeDimensionSpec]:
        agg_time_dimension_specs = self._get_agg_time_dimension_specs_for_metric(metric_reference)
        distinct_agg_time_dimension_identifiers = set(
            [(spec.reference, spec.entity_links) for spec in agg_time_dimension_specs]
        )
        if len(distinct_agg_time_dimension_identifiers) != 1:
            # If the metric's input measures have different agg_time_dimensions, user must use metric_time.
            return []

        agg_time_dimension_reference, agg_time_dimension_entity_links = distinct_agg_time_dimension_identifiers.pop()
        valid_agg_time_dimension_specs = TimeDimensionSpec.generate_possible_specs_for_time_dimension(
            time_dimension_reference=agg_time_dimension_reference,
            entity_links=agg_time_dimension_entity_links,
            custom_granularities=self._custom_granularities,
        )
        return valid_agg_time_dimension_specs

    def get_min_queryable_time_granularity(self, metric_reference: MetricReference) -> TimeGranularity:
        """The minimum grain that can be queried with this metric.

        Maps to the largest granularity defined for any of the metric's agg_time_dimensions.
        """
        result = self._metric_reference_to_min_metric_time_grain.get(metric_reference)
        if result is not None:
            return result

        result = self._get_min_queryable_time_granularity(metric_reference)
        self._metric_reference_to_min_metric_time_grain[metric_reference] = result
        return result

    def _get_min_queryable_time_granularity(self, metric_reference: MetricReference) -> TimeGranularity:
        metric = self.get_metric(metric_reference)
        agg_time_dimension_grains = set()
        for input_measure in metric.input_measures:
            measure_properties = self._semantic_model_lookup.measure_lookup.get_properties(
                input_measure.measure_reference
            )
            agg_time_dimension_grains.add(measure_properties.agg_time_granularity)

        return max(agg_time_dimension_grains, key=lambda time_granularity: time_granularity.to_int())
