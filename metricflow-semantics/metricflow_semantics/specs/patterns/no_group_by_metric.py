from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

from typing_extensions import override

from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.specs.instance_spec import InstanceSpec, LinkableInstanceSpec
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern
from metricflow_semantics.specs.spec_set import group_specs_by_type


@dataclass(frozen=True)
class NoGroupByMetricPattern(SpecPattern):
    """Matches to linkable specs, but only if they're not group by metrics.

    Group by metrics are allowed in filters but not in the query input group by.
    """

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        specs_to_return: List[LinkableInstanceSpec] = []
        spec_set = group_specs_by_type(candidate_specs)
        specs_to_return.extend(spec_set.time_dimension_specs)
        specs_to_return.extend(spec_set.dimension_specs)
        specs_to_return.extend(spec_set.entity_specs)

        return specs_to_return

    @property
    @override
    def element_pre_filter(self) -> LinkableElementFilter:
        return LinkableElementFilter(without_any_of=frozenset({LinkableElementProperty.METRIC}))
