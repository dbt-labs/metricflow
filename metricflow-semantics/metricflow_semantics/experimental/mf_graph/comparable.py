from __future__ import annotations

import functools
from abc import ABC, abstractmethod
from typing import Union

from typing_extensions import override

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.comparison_helpers import ComparisonOtherType, SupportsLessThan

ComparisonKey = AnyLengthTuple[Union[str, int, float, SupportsLessThan]]


@functools.total_ordering
class Comparable(SupportsLessThan, ABC):
    """Mixin class that allows classes of any type to be compared.

    This is useful sorting nodes and edges where the nodes and edges may be different classes.
    """

    @property
    @abstractmethod
    def comparison_key(self) -> ComparisonKey:
        """Return a tuple that can be used for comparing this with any other `Comparable`.

        The tuple consists of the implementing class name followed by the items that should be used for comparing
        instances of the implementing classes.
        """
        raise NotImplementedError

    @override
    def __lt__(self, other: ComparisonOtherType) -> bool:
        if not isinstance(other, Comparable):
            return NotImplemented

        # Consider adding the module name to handle cases where two classes implementing this have the same name.
        return (self.__class__.__name__,) + self.comparison_key < ((other.__class__.__name__,) + other.comparison_key)
