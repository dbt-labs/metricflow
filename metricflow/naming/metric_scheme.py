from __future__ import annotations

from typing import Optional

from dbt_semantic_interfaces.references import MetricReference
from typing_extensions import override

from metricflow.naming.naming_scheme import QueryItemNamingScheme
from metricflow.specs.patterns.metric_pattern import MetricSpecPattern
from metricflow.specs.specs import (
    InstanceSpec,
    InstanceSpecSet,
)


class MetricNamingScheme(QueryItemNamingScheme):
    """A naming scheme for metrics."""

    @override
    def input_str(self, instance_spec: InstanceSpec) -> Optional[str]:
        spec_set = InstanceSpecSet.from_specs((instance_spec,))
        names = tuple(spec.element_name for spec in spec_set.metric_specs)

        if len(names) != 1:
            raise RuntimeError(f"Did not get 1 name for {instance_spec}. Got {names}")

        return names[0]

    @override
    def spec_pattern(self, input_str: str) -> MetricSpecPattern:
        input_str = input_str.lower()
        if not self.input_str_follows_scheme(input_str):
            raise RuntimeError(f"{repr(input_str)} does not follow this scheme.")
        return MetricSpecPattern(metric_reference=MetricReference(element_name=input_str))

    @override
    def input_str_follows_scheme(self, input_str: str) -> bool:
        # TODO: Use regex.
        return True

    @override
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id()={hex(id(self))})"
