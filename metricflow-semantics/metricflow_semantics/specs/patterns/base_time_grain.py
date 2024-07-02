from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Set

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


class DefaultTimeGranularityPattern(SpecPattern):
    """A pattern that matches linkable specs, but for time dimension specs, only the one with the default granularity.

    When queried with metrics, default_granularity is specified in the YAML spec for the metrics. If this field is not
    set for any of the queried metric(s), defaults to DAY for those metrics unless the minimum granularity is larger than
    DAY. In that case, defaults to the smallest available granularity.

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

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[InstanceSpec]:
        spec_set = group_specs_by_type(candidate_specs)

        spec_key_to_grains: Dict[TimeDimensionSpecComparisonKey, Set[TimeGranularity]] = defaultdict(set)
        spec_key_to_specs: Dict[TimeDimensionSpecComparisonKey, List[TimeDimensionSpec]] = defaultdict(list)
        for time_dimension_spec in spec_set.time_dimension_specs:
            spec_key = time_dimension_spec.comparison_key(exclude_fields=(TimeDimensionSpecField.TIME_GRANULARITY,))
            spec_key_to_grains[spec_key].add(time_dimension_spec.time_granularity)
            spec_key_to_specs[spec_key].append(time_dimension_spec)

        matched_time_dimension_specs: List[TimeDimensionSpec] = []
        for spec_key, time_grains in spec_key_to_grains.items():
            # Replace this with new default logic! How does it know for metric time what grain is avail?
            # Maybe that logic is done already when passed in here?
            # But now we need separate logic if metrics are queried, so we'll need to pass in metrics here (optionally).
            matched_time_dimension_specs.append(spec_key_to_specs[spec_key][0].with_grain(min(time_grains)))

        matching_specs: Sequence[LinkableInstanceSpec] = (
            spec_set.dimension_specs
            + tuple(matched_time_dimension_specs)
            + spec_set.entity_specs
            + spec_set.group_by_metric_specs
        )

        return matching_specs
