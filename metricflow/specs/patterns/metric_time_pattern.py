from __future__ import annotations

from typing import Sequence

from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from typing_extensions import override

from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.specs import (
    InstanceSpec,
    InstanceSpecSet,
    TimeDimensionSpec,
)


class MetricTimePattern(SpecPattern):
    """Pattern that matches to only metric_time specs.

    This pattern can be used to check if metric_time has been specified in a query, or to help implement checks that
    only apply to metric_time specs.
    """

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[TimeDimensionSpec]:
        return tuple(
            time_dimension_spec
            for time_dimension_spec in InstanceSpecSet.from_specs(candidate_specs).time_dimension_specs
            if time_dimension_spec.element_name == METRIC_TIME_ELEMENT_NAME
        )
