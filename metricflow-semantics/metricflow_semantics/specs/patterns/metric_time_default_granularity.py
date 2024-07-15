from __future__ import annotations

from collections import defaultdict
from typing import Dict, Optional, Sequence, Set, Tuple

from dbt_semantic_interfaces.type_enums import TimeGranularity
from typing_extensions import override

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
    """A pattern that matches metric_time specs if they have the default granularity for the requested metrics.

    This is used to determine the granularity that should be used for metric_time if no granularity is specified.
    Spec passes through if granularity is already selected or if no metrics were queried, since no default is needed.
    All non-metric_time specs are passed through.

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
    """

    def __init__(self, max_metric_default_time_granularity: Optional[TimeGranularity]) -> None:  # noqa: D107
        self._max_metric_default_time_granularity = max_metric_default_time_granularity

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[InstanceSpec]:
        spec_set = group_specs_by_type(candidate_specs)

        # If there are no metrics or metric_time specs in the query, skip this filter.
        if not (self._max_metric_default_time_granularity and spec_set.metric_time_specs):
            return candidate_specs

        spec_key_to_grains: Dict[TimeDimensionSpecComparisonKey, Set[TimeGranularity]] = defaultdict(set)
        spec_key_to_specs: Dict[TimeDimensionSpecComparisonKey, Tuple[TimeDimensionSpec, ...]] = defaultdict(tuple)
        for metric_time_spec in spec_set.metric_time_specs:
            spec_key = metric_time_spec.comparison_key(exclude_fields=(TimeDimensionSpecField.TIME_GRANULARITY,))
            spec_key_to_grains[spec_key].add(metric_time_spec.time_granularity)
            spec_key_to_specs[spec_key] += (metric_time_spec,)

        matched_metric_time_specs: Tuple[TimeDimensionSpec, ...] = ()
        for spec_key, time_grains in spec_key_to_grains.items():
            if self._max_metric_default_time_granularity in time_grains:
                matched_metric_time_specs += (
                    spec_key_to_specs[spec_key][0].with_grain(self._max_metric_default_time_granularity),
                )
            else:
                # If default_granularity is not in the available options, then time granularity was specified in the request
                # and a default is not needed here. Pass all options through for this spec key.
                matched_metric_time_specs += spec_key_to_specs[spec_key]

        matching_specs: Sequence[LinkableInstanceSpec] = (
            spec_set.dimension_specs
            + matched_metric_time_specs
            + tuple(spec for spec in spec_set.time_dimension_specs if not spec.is_metric_time)
            + spec_set.entity_specs
            + spec_set.group_by_metric_specs
        )

        return matching_specs
