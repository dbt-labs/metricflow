from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import override

from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.specs import (
    InstanceSpec,
    InstanceSpecSet,
    MetricSpec,
)


@dataclass(frozen=True)
class MetricSpecPattern(SpecPattern):
    """Matches MetricSpecs that have the given metric_reference."""

    metric_reference: MetricReference

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[MetricSpec]:
        spec_set = InstanceSpecSet.from_specs(candidate_specs)
        return tuple(
            metric_name for metric_name in spec_set.metric_specs if metric_name.reference == self.metric_reference
        )
