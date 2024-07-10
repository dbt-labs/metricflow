from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Set

from dbt_semantic_interfaces.type_enums import TimeGranularity
from typing_extensions import override

from metricflow_semantics.specs.instance_spec import InstanceSpec, LinkableInstanceSpec
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern
from metricflow_semantics.specs.spec_set import group_specs_by_type
from metricflow_semantics.specs.time_dimension_spec import (
    TimeDimensionSpec,
    TimeDimensionSpecComparisonKey,
    TimeDimensionSpecField,
)


class MinimumTimeGrainPattern(SpecPattern):
    """A pattern that matches linkable specs, but for time dimension specs, only the one with the finest grain.

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

    This pattern helps to implement matching of group-by-items for where filters - in those cases, an ambiguously
    specified group-by-item can only match to time dimension spec with the base grain.

    Also, this is currently used to help implement restrictions on cumulative metrics where they can only be queried
    by the base grain of metric_time.
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
            matched_time_dimension_specs.append(spec_key_to_specs[spec_key][0].with_grain(min(time_grains)))

        matching_specs: Sequence[LinkableInstanceSpec] = (
            spec_set.dimension_specs
            + tuple(matched_time_dimension_specs)
            + spec_set.entity_specs
            + spec_set.group_by_metric_specs
        )

        return matching_specs
