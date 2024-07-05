from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

from typing_extensions import override

from metricflow_semantics.specs.instance_spec import InstanceSpec, LinkableInstanceSpec
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern
from metricflow_semantics.specs.spec_set import group_specs_by_type


@dataclass(frozen=True)
class NoneDatePartPattern(SpecPattern):
    """Matches to linkable specs, but for time dimension specs, only matches to ones without date_part.

    This is used to help implement restrictions for cumulative metrics where those can not be queried by date_part.
    """

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        specs_to_return: List[LinkableInstanceSpec] = []
        spec_set = group_specs_by_type(candidate_specs)
        for time_dimension_spec in spec_set.time_dimension_specs:
            if time_dimension_spec.date_part is None:
                specs_to_return.append(time_dimension_spec)
        specs_to_return.extend(spec_set.dimension_specs)
        specs_to_return.extend(spec_set.entity_specs)
        specs_to_return.extend(spec_set.group_by_metric_specs)

        return specs_to_return
