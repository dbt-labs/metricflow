from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from metricflow.naming.naming_scheme import QueryItemNamingScheme
from metricflow.specs.patterns.spec_pattern import SpecPattern


@dataclass(frozen=True)
class InputPatternDescription:
    """Describes the pattern / naming scheme associated with a query input.

    Some query inputs (e.g. group_by_names=['listing__county']) are converted into spec patterns through a naming
    scheme. It's useful in some cases to know the naming scheme that was used to generate the spec pattern (e.g. when
    generating suggestions, we want to use the same naming scheme as the input), so this class groups them.
    """

    naming_scheme: QueryItemNamingScheme
    spec_pattern: SpecPattern


class MetricFlowQueryResolverInput(ABC):
    """Base class for all inputs to the query resolver."""

    @property
    @abstractmethod
    def ui_description(self) -> str:
        """A string that can be used to describe the input in user-facing cases."""
        raise NotImplementedError

    @property
    def input_pattern_description(self) -> Optional[InputPatternDescription]:
        """If applicable to this input, return the spec pattern and naming scheme associated with this input.

        Inputs like the metrics and group-by-items would have this set, but inputs like the filter would not.
        """
        return None
