from __future__ import annotations

import functools
from abc import ABC, abstractmethod
from typing import Any, Protocol, Tuple, Union


class SupportsLessThan(Protocol):
    """Protocol describing an object that supports `<`.

    This should be replaced with an already-available implementation.
    """

    def __lt__(self, other: ComparisonAnyType) -> bool:
        ...


# Type used to annotate the `other` argument in standard comparison methods like `__lt__`.
# Helpful to reduce the appearance of `# type: ignore`.
ComparisonAnyType = Any  # type: ignore


ComparisonKey = Tuple[Union[str, int, float, SupportsLessThan], ...]


@functools.total_ordering
class Comparable(ABC):
    @property
    @abstractmethod
    def comparison_key(self) -> ComparisonKey:
        raise NotImplementedError

    def __lt__(self, other: ComparisonAnyType) -> bool:
        """Standard Python `<` comparison."""
        if not isinstance(other, Comparable):
            return NotImplemented

        # Consider adding the module name to handle cases where two classes implementing this have the same name.
        return (self.__class__.__name__,) + self.comparison_key < ((other.__class__.__name__,) + other.comparison_key)
