from __future__ import annotations

from typing import Sequence

from typing_extensions import override

from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.specs import InstanceSpec


class MatchListSpecPattern(SpecPattern):
    """A spec pattern that matches based on a configured list of specs.

    This is useful for filtering possible group-by-items to ones valid for a query.
    """

    def __init__(self, listed_specs: Sequence[InstanceSpec]) -> None:  # noqa: D
        self._listed_specs = set(listed_specs)

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[InstanceSpec]:
        return tuple(spec for spec in candidate_specs if spec in self._listed_specs)
