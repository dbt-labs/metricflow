from __future__ import annotations

from typing import List, Sequence

from typing_extensions import override

from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.specs import (
    InstanceSpec,
    InstanceSpecSet,
    LinkableInstanceSpec,
)


class NoneDatePartPattern(SpecPattern):
    """Matches to linkable specs, but for time dimension specs, only matches to ones without date_part.

    This is used to help implement restrictions for cumulative metrics where those can not be queried by date_part.
    """

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        specs_to_return: List[LinkableInstanceSpec] = []
        spec_set = InstanceSpecSet.from_specs(candidate_specs)
        for time_dimension_spec in spec_set.time_dimension_specs:
            if time_dimension_spec.date_part is None:
                specs_to_return.append(time_dimension_spec)
        specs_to_return.extend(spec_set.dimension_specs)
        specs_to_return.extend(spec_set.entity_specs)

        return specs_to_return
