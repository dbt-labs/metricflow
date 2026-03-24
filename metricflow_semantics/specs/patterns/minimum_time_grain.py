from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Sequence, Set, Tuple

from typing_extensions import override

from metricflow_semantics.specs.instance_spec import InstanceSpec, LinkableInstanceSpec
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern
from metricflow_semantics.specs.spec_set import group_specs_by_type
from metricflow_semantics.specs.time_dimension_spec import (
    TimeDimensionSpec,
    TimeDimensionSpecComparisonKey,
    TimeDimensionSpecField,
)
from metricflow_semantics.time.granularity import ExpandedTimeGranularity


@dataclass(frozen=True)
class MinimumTimeGrainPattern(SpecPattern):
    """A pattern that matches linkable specs, but for time dimension specs, only the one with the finest base grain.

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
    of the source. For custom granularities, this means the base grain associated with the time dimension spec.

    This pattern helps to implement matching of group-by-items for where filters - in those cases, an ambiguously
    specified group-by-item can only match to time dimension spec with the base grain.
    """

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[InstanceSpec]:
        spec_set = group_specs_by_type(candidate_specs)

        time_dimension_specs_with_no_grain: Tuple[TimeDimensionSpec, ...] = ()
        spec_key_to_grains: Dict[TimeDimensionSpecComparisonKey, Set[ExpandedTimeGranularity]] = defaultdict(set)
        spec_key_to_specs: Dict[TimeDimensionSpecComparisonKey, List[TimeDimensionSpec]] = defaultdict(list)
        for time_dimension_spec in spec_set.time_dimension_specs:
            if time_dimension_spec.time_granularity is None:
                time_dimension_specs_with_no_grain += (time_dimension_spec,)
                continue
            spec_key = time_dimension_spec.comparison_key(exclude_fields=(TimeDimensionSpecField.TIME_GRANULARITY,))
            spec_key_to_grains[spec_key].add(time_dimension_spec.time_granularity)
            spec_key_to_specs[spec_key].append(time_dimension_spec)

        matched_time_dimension_specs: List[TimeDimensionSpec] = []
        for spec_key, time_grains in spec_key_to_grains.items():
            sorted_time_grains = sorted(
                time_grains,
                # Sort by smallest to largest standard granularity, with custom granularities last (sorted by their
                # base granularity) since we don't know how large they are.
                key=lambda grain: (grain.is_custom_granularity, grain.base_granularity.to_int()),
            )
            assert sorted_time_grains, (
                f"Each time dimension spec should have at least one grain, but none was found for spec_key {spec_key}. "
                "This indicates internal misconfiguration."
            )
            matched_time_dimension_specs.append(spec_key_to_specs[spec_key][0].with_grain(sorted_time_grains[0]))

        matching_specs: Sequence[LinkableInstanceSpec] = (
            spec_set.dimension_specs
            + tuple(matched_time_dimension_specs)
            + time_dimension_specs_with_no_grain
            + spec_set.entity_specs
            + spec_set.group_by_metric_specs
        )

        return matching_specs
