from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from typing_extensions import override

from metricflow.specs.patterns.spec_pattern import QueryItemNamingScheme, ScoringResults, SpecPattern
from metricflow.specs.specs import MetricSpec


@dataclass(frozen=True)
class MetricNamePattern(SpecPattern[MetricSpec]):
    """A pattern that matches specs on the element name."""

    naming_scheme: QueryItemNamingScheme[MetricSpec]
    target_spec: MetricSpec
    input_str: str

    @override
    def score(self, candidate_specs: Sequence[MetricSpec]) -> ScoringResults:
        matching_specs = []

        for candidate_spec in candidate_specs:
            if candidate_spec.element_name == self.target_spec.element_name:
                matching_specs.append(candidate_spec)

        return self.make_scoring_results(
            matching_specs=matching_specs,
            non_matching_specs=tuple(spec for spec in candidate_specs if spec not in matching_specs),
            input_str=self.input_str,
            naming_scheme=MetricNamingScheme(),
        )


class MetricNamingScheme(QueryItemNamingScheme[MetricSpec]):
    """A naming scheme for metric specs using the element name."""

    @override
    def input_str(self, instance_spec: MetricSpec) -> str:
        return instance_spec.element_name

    @override
    def output_column_name(self, instance_spec: MetricSpec) -> str:
        return instance_spec.element_name

    @override
    def spec_pattern(self, input_str: str) -> SpecPattern[MetricSpec]:
        if not self.input_str_follows_scheme(input_str):
            raise ValueError("Can't create a pattern as the input string does not follow this scheme.")
        return MetricNamePattern(
            naming_scheme=self,
            target_spec=MetricSpec(element_name=input_str),
            input_str=input_str,
        )

    @override
    def input_str_follows_scheme(self, input_str: str) -> bool:
        # Could use UniqueAndValidNameRule, but needs some modifications to that class.
        return True

    @property
    @override
    def input_str_description(self) -> str:
        return "The metric input string should follow the convention for defining metric names in the configuration."
