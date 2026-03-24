from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, Tuple

from typing_extensions import override

from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern


@dataclass(frozen=True)
class MatchListSpecPattern(SpecPattern):
    """A spec pattern that matches based on a configured list of specs.

    This is useful for filtering possible group-by-items to ones valid for a query.
    """

    listed_specs: Tuple[InstanceSpec, ...]

    @staticmethod
    def create(listed_specs: Sequence[InstanceSpec]) -> MatchListSpecPattern:  # noqa: D102
        return MatchListSpecPattern(tuple(listed_specs))

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[InstanceSpec]:
        return tuple(spec for spec in candidate_specs if spec in self.listed_specs)
