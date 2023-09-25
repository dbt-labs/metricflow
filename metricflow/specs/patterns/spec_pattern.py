from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Sequence, TypeVar

from dbt_semantic_interfaces.pretty_print import pformat_big_objects

from metricflow.specs.specs import InstanceSpec


class QueryInterfaceItemNamingScheme(ABC):
    """Describes how to name items in the inputs and outputs of a query.

    For example, a user needs to input strings that specify the metrics and group by items. These can be in different
    formats like 'user__country' or "TimeDimension('metric_time', 'DAY')"
    """

    @abstractmethod
    def input_str(self, instance_spec: InstanceSpec) -> Optional[str]:
        """Following this scheme, return the string that can be used as an input that would specify the given spec."""
        pass

    @abstractmethod
    def output_column_str(self, instance_spec: InstanceSpec) -> str:
        """Following this scheme, return the name of the column containing the item with the given spec."""
        pass

    @abstractmethod
    def spec_pattern(self, input_str: str) -> SpecPattern:
        """Given that the input follows this scheme, return a spec pattern that can be used to resolve a query."""
        pass

    @abstractmethod
    def is_valid_input_str(self, input_str: str) -> bool:
        """Returns true if the given input string follows this naming scheme."""
        pass


@dataclass
class ScoredSpec:
    """The result of matching a spec pattern to a spec."""

    # The spec associated with the score.
    spec: InstanceSpec
    # A float indicating how well the spec matches the pattern. A score > 0 indicates that the spec matches
    # the pattern. A score <= 0 indicates that the spec does not match the pattern, but the more negative it is,
    # the worse the match. This is used to rank suggestions when a user-provided spec pattern does not match any
    # know specs. e.g. a spec pattern made from user input that is supposed to match a metric with a specific name
    # may not produce a match if there is a typo. This score can be used with the pattern to provided suggestions
    # based on known metric names.
    score: float

    @property
    def matches(self) -> bool:  # noqa: D
        return self.score > 0


SelfTypeT = TypeVar("SelfTypeT", bound="SpecPattern")


@dataclass(frozen=True)
class ScoringResults:
    """The results of matching a pattern to a set of specs."""

    scored_specs: Sequence[ScoredSpec]

    @property
    def matched_specs(self) -> Sequence[InstanceSpec]:  # noqa: D
        return tuple(scored_spec.spec for scored_spec in self.scored_specs if scored_spec.matches)

    @property
    def has_one_match(self) -> bool:  # noqa: D
        return len(self.matched_specs) == 1

    @property
    def matching_spec(self) -> InstanceSpec:
        """If there is exactly one spec that matched, return it. Otherwise, raise a RuntimeError."""
        matched_specs = self.matched_specs
        if len(matched_specs) == 1:
            raise RuntimeError(
                f"This result not contain a spec that matches. Got:\n{pformat_big_objects(self.scored_specs)}"
            )
        return matched_specs[0]


class SpecPattern(ABC):
    """A pattern is used to select a spec from a group of specs based on class-defined criteria."""

    @abstractmethod
    def score(self, candidate_specs: Sequence[InstanceSpec]) -> ScoringResults:
        """Given a group of instance specs, try to match them to this pattern and return the associated scores."""
        pass
