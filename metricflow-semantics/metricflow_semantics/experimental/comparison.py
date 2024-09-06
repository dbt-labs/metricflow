from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Tuple

# Type used to annotate the `other` argument in standard comparison methods like `__lt__`.
# Helpful to reduce the appearance of `# type: ignore`.
ComparisonAnyType = Any  # type: ignore


# @dataclass(frozen=True)
# class ComparisonKey:
#     class_name: str
#     additional_fields: Tuple[str, ...]
#
#     @property
#     def comparison_tuple(self):

ComparisonTuple = Tuple[ComparisonAnyType, ...]


class Comparable(ABC):
    @property
    @abstractmethod
    def comparison_tuple(self) -> ComparisonTuple:
        raise NotImplementedError

    def __lt__(self, other: ComparisonAnyType) -> bool:
        if not isinstance(other, Comparable):
            return NotImplemented
        if not isinstance(other, self.__class__):
            return self.__class__.__name__ < other.__class__.__name__
        return self.comparison_tuple < other.comparison_tuple


@dataclass(frozen=True)
class StringComparable(Comparable):
    str_value: str

    @property
    def comparison_tuple(self) -> ComparisonTuple:
        return (self,)

    def __lt__(self, other: ComparisonAnyType) -> bool:
        if not isinstance(other, Comparable):
            return NotImplemented
        if not isinstance(other, self.__class__):
            return self.__class__.__name__ < other.__class__.__name__
        return self.str_value < other.str_value
