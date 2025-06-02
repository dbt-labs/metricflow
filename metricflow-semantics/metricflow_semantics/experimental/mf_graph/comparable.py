from __future__ import annotations

import functools
from abc import ABC, abstractmethod
from typing import Union

from typing_extensions import override

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.comparison_helpers import ComparisonOtherType, SupportsLessThan

ComparisonKey = AnyLengthTuple[Union[None, str, int, float, SupportsLessThan]]


@functools.total_ordering
class Comparable(SupportsLessThan, ABC):
    """Mixin class that allows classes of any type to be compared.

    This is useful sorting nodes and edges where the nodes and edges may be different classes.
    Implementing classes should ensure that the `comparison_key` aligns with how `__eq__` is defined.
    """

    @property
    @abstractmethod
    def comparison_key(self) -> ComparisonKey:
        """Return a tuple that can be used for comparing different instances of this class."""
        raise NotImplementedError

    @override
    def __lt__(self, other: ComparisonOtherType) -> bool:
        if not isinstance(other, Comparable):
            return NotImplemented

        self_comparison_key = (self.__class__.__module__, self.__class__.__qualname__, *self.comparison_key)
        other_comparison_key = (other.__class__.__module__, other.__class__.__qualname__, *other.comparison_key)

        return self_comparison_key < other_comparison_key
