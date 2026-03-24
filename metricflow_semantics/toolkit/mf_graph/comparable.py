from __future__ import annotations

import functools
import itertools
import logging
from abc import ABC, abstractmethod
from typing import Union

from typing_extensions import override

from metricflow_semantics.toolkit.comparison_helpers import ComparisonOtherType, SupportsLessThan
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple

logger = logging.getLogger(__name__)


ComparisonKey = AnyLengthTuple[Union[None, SupportsLessThan]]


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

        self_comparison_key: AnyLengthTuple[ComparisonOtherType] = (
            self.__class__.__module__,
            self.__class__.__qualname__,
            *self.comparison_key,
        )
        other_comparison_key: AnyLengthTuple[ComparisonOtherType] = (
            other.__class__.__module__,
            other.__class__.__qualname__,
            *other.comparison_key,
        )

        for self_key_item, other_key_item in itertools.zip_longest(self_comparison_key, other_comparison_key):
            if self_key_item is None:
                return True
            elif other_key_item is None:
                return False
            elif self_key_item < other_key_item:
                return True
            elif self_key_item > other_key_item:
                return False
            # Must be equal, so compare the next item.

        return False
