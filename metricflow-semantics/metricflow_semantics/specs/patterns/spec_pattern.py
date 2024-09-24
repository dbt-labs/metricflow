from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, FrozenSet, Sequence

from metricflow_semantics.model.linkable_element_property import LinkableElementProperty

if TYPE_CHECKING:
    from metricflow_semantics.specs.instance_spec import InstanceSpec


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

    @property
    def without_linkable_element_properties(self) -> FrozenSet[LinkableElementProperty]:
        """Returns the set of properties of linkable elements that this won't match."""
        return frozenset()
