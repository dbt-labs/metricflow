from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Set

from dbt_semantic_interfaces.references import MetricReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from typing_extensions import override

from metricflow_semantics.model.semantics.metric_lookup import MetricLookup
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern
from metricflow_semantics.specs.spec_classes import (
    InstanceSpec,
    LinkableInstanceSpec,
    TimeDimensionSpec,
    TimeDimensionSpecComparisonKey,
    TimeDimensionSpecField,
)
from metricflow_semantics.specs.spec_set import group_specs_by_type


class MetricTimeDefaultGranularityPattern(SpecPattern):
    """A pattern that matches metric_time specs, applying the default granularity for the requested metrics.

    If granularity is already selected or if no metrics were queried, no default is needed. All other specs are passed through.

    e.g., if a metric with default_granularity MONTH is queried

    inputs:
        [
            TimeDimensionSpec('metric_time', 'day'),
            TimeDimensionSpec('metric_time', 'week'),
            TimeDimensionSpec('metric_time', 'month'),
            DimensionSpec('listing__country'),
        ]

    matches:
        [
            TimeDimensionSpec('metric_time', 'month'),
            DimensionSpec('listing__country'),
        ]

    The finest grain represents the defined grain of the time dimension in the semantic model when evaluating specs
    of the source.

    This pattern helps to implement matching of group-by-items. An ambiguously specified group-by-item can only match to
    time dimension spec with the base grain.
    """

    def __init__(self, metric_lookup: MetricLookup, queried_metrics: Sequence[MetricReference] = ()) -> None:
        """Match only time dimensions with the default granularity for a given query.

        Only affects time dimensions. All other items pass through.
        """
        self._metric_lookup = metric_lookup
        self._queried_metrics = queried_metrics

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[InstanceSpec]:
        default_granularity_for_metrics = self._metric_lookup.get_default_granularity_for_metrics(self._queried_metrics)
        spec_set = group_specs_by_type(candidate_specs)
        # If there are no metrics or metric_time specs in the query, skip this filter.
        if not (default_granularity_for_metrics and spec_set.metric_time_specs):
            return candidate_specs

        spec_key_to_grains: Dict[TimeDimensionSpecComparisonKey, Set[TimeGranularity]] = defaultdict(set)
        spec_key_to_specs: Dict[TimeDimensionSpecComparisonKey, List[TimeDimensionSpec]] = defaultdict(list)
        for metric_time_spec in spec_set.metric_time_specs:
            spec_key = metric_time_spec.comparison_key(exclude_fields=(TimeDimensionSpecField.TIME_GRANULARITY,))
            spec_key_to_grains[spec_key].add(metric_time_spec.time_granularity)
            spec_key_to_specs[spec_key].append(metric_time_spec)

        matched_metric_time_specs: List[TimeDimensionSpec] = []
        for spec_key, time_grains in spec_key_to_grains.items():
            if default_granularity_for_metrics in time_grains:
                matched_metric_time_specs.append(
                    spec_key_to_specs[spec_key][0].with_grain(default_granularity_for_metrics)
                )
            else:
                # If default_granularity is not in the available options, then time granularity was specified in the request
                # and a default is not needed here. Pass all options through.
                matched_metric_time_specs.extend(spec_key_to_specs[spec_key])

        matching_specs: Sequence[LinkableInstanceSpec] = (
            spec_set.dimension_specs
            + tuple(matched_metric_time_specs)
            + tuple(spec for spec in spec_set.time_dimension_specs if not spec.is_metric_time)
            + spec_set.entity_specs
            + spec_set.group_by_metric_specs
        )

        return matching_specs
