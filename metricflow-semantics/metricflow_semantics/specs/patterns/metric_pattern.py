from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

from typing_extensions import override

from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.patterns.entity_link_pattern import SpecPatternParameterSet
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern
from metricflow_semantics.specs.spec_set import group_specs_by_type


@dataclass(frozen=True)
class MetricSpecPattern(SpecPattern):
    """Matches MetricSpecs that have the given metric_reference."""

    parameter_set: SpecPatternParameterSet

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[MetricSpec]:
        filtered_candidate_specs = group_specs_by_type(candidate_specs).metric_specs
        keys_to_check = set(field_to_compare.value for field_to_compare in self.parameter_set.fields_to_compare)

        matching_specs: List[MetricSpec] = []
        parameter_set_values = tuple(getattr(self.parameter_set, key_to_check) for key_to_check in keys_to_check)
        for spec in filtered_candidate_specs:
            spec_values = tuple(getattr(spec, key_to_check, None) for key_to_check in keys_to_check)
            if spec_values == parameter_set_values:
                matching_specs.append(spec)

        return matching_specs
