from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence

from metricflow.specs.specs import InstanceSpec


class SpecPattern(ABC):
    """A pattern is used to select specs from a group of candidate specs based on class-defined criteria.

    This could be named SpecFilter as well, but a filter is often used in the context of the WhereFilter.
    """

    @abstractmethod
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[InstanceSpec]:
        """Given candidate specs, return the ones that match this pattern."""
        raise NotImplementedError

    def matches_any(self, candidate_specs: Sequence[InstanceSpec]) -> bool:
        """Returns true if this spec matches any of the given specs."""
        return len(self.match(candidate_specs)) > 0
