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


class DefaultTimeGranularityPattern(SpecPattern):
    """A pattern that matches linkable specs, but for time dimension specs, only the one with the default granularity.

    # TODO: this might need to change - the below is only relevant for metric_time, right? Update this for all dims.
    When queried with metrics, default_granularity is specified in the YAML spec for the metrics. If this field is not
    set for a metric, it will default to DAY unless the minimum granularity is larger than DAY, in which case it will
    default to the minimum granularity. When multiple metrics are queried, the default granularity for the query will
    be the largest of the default granularities for the metrics queried.

    Same defaults apply if no metrics are queried: default to DAY if available for the time dimension queried, else the
    smallest available granularity. Always defaults to DAY for metric_time if not metrics are queried.

    e.g.

    inputs:
        [
            TimeDimensionSpec('metric_time', 'day'),
            TimeDimensionSpec('metric_time', 'month.'),
            DimensionSpec('listing__country'),
        ]

    matches:
        [
            TimeDimensionSpec('metric_time', 'day'),
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
        spec_set = group_specs_by_type(candidate_specs)

        spec_key_to_grains: Dict[TimeDimensionSpecComparisonKey, Set[TimeGranularity]] = defaultdict(set)
        spec_key_to_specs: Dict[TimeDimensionSpecComparisonKey, List[TimeDimensionSpec]] = defaultdict(list)
        for time_dimension_spec in spec_set.time_dimension_specs:
            spec_key = time_dimension_spec.comparison_key(exclude_fields=(TimeDimensionSpecField.TIME_GRANULARITY,))
            spec_key_to_grains[spec_key].add(time_dimension_spec.time_granularity)
            spec_key_to_specs[spec_key].append(time_dimension_spec)

        default_granularity_for_metrics = self._metric_lookup.get_default_granularity_for_metrics(self._queried_metrics)
        print("default::", self._queried_metrics, default_granularity_for_metrics)
        matched_time_dimension_specs: List[TimeDimensionSpec] = []
        for spec_key, time_grains in spec_key_to_grains.items():
            grain_to_use = (
                default_granularity_for_metrics
                if (default_granularity_for_metrics and spec_key.source_spec.is_metric_time)
                else min(time_grains)
            )
            matched_time_dimension_specs.append(spec_key_to_specs[spec_key][0].with_grain(grain_to_use))

        matching_specs: Sequence[LinkableInstanceSpec] = (
            spec_set.dimension_specs
            + tuple(matched_time_dimension_specs)
            + spec_set.entity_specs
            + spec_set.group_by_metric_specs
        )

        return matching_specs
