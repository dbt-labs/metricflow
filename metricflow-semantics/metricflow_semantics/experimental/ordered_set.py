from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Hashable, MutableSet, Set
from functools import cached_property
from typing import Generic, Iterable, Iterator, Optional, TypeVar

from typing_extensions import Self, override

from metricflow_semantics.collection_helpers.mf_type_aliases import HashableT, HashableT_co
from metricflow_semantics.experimental.comparison_helpers import ComparisonOtherType

logger = logging.getLogger(__name__)

OrderedSetT = TypeVar("OrderedSetT", bound="OrderedSet", covariant=True)


class OrderedSet(Generic[HashableT_co], Set[HashableT_co], ABC):
    """Set where the iteration order is the insertion order.

    * Having a consistent iteration order is helpful for ensuring consistency in tests and snapshot generation without
      sorting.
    * Since this is meant to be used for consistency in iteration, eq / hash behavior does not depend on order.
    * Considering using the `ordered-set` package instead, but it would add a dependency and this implementation is
      short.
    """

    def __init__(
        self,
        iterable: Optional[Iterable[HashableT_co]] = None,
        _set_as_dict: Optional[dict[HashableT_co, None]] = None,
    ) -> None:
        """Initializer.

        Args:
            iterable: Create the set using the given iterable.
            _set_as_dict: For internal use - create a copy of this and use it for the internal representation.
        """
        self._set_as_dict = _set_as_dict.copy() if _set_as_dict is not None else {}
        if iterable is not None:
            self._set_as_dict.update({item: None for item in iterable})

    def intersection(self, other: Iterable[HashableT_co]) -> Self:  # noqa: D102
        other_set = set(other)
        return self.__class__(item for item in self if item in other_set)

    def union(self, other: Iterable[HashableT_co]) -> Self:  # noqa: D102
        return self.__class__(tuple(self._set_as_dict.keys()) + tuple(other))

    @override
    def __contains__(self, obj: ComparisonOtherType) -> bool:
        return obj in self._set_as_dict

    @override
    def __len__(self) -> int:
        return len(self._set_as_dict)

    @override
    def __iter__(self) -> Iterator[HashableT_co]:
        return iter(self._set_as_dict)

    @override
    def __str__(self) -> str:
        return "{" + ", ".join(str(item) for item in self) + "}"

    @abstractmethod
    def as_mutable(self) -> MutableOrderedSet[HashableT_co]:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def as_frozen(self) -> FrozenOrderedSet[HashableT_co]:  # noqa: D102
        raise NotImplementedError


class FrozenOrderedSet(Generic[HashableT_co], OrderedSet[HashableT_co], Hashable):
    """A frozen version of `OrderedSet` that can be hashed."""

    @cached_property
    def _cached_hash_value(self) -> int:
        return hash(frozenset(self._set_as_dict.keys()))

    @override
    def __hash__(self) -> int:
        return self._cached_hash_value

    @override
    def as_mutable(self) -> MutableOrderedSet[HashableT_co]:
        return MutableOrderedSet(self)

    @override
    def as_frozen(self) -> FrozenOrderedSet[HashableT_co]:
        return self

    @override
    def union(self, other: Iterable[HashableT_co]) -> Self:  # noqa: D102
        return super().union(other)


class MutableOrderedSet(Generic[HashableT], OrderedSet[HashableT], MutableSet[HashableT]):
    """An ordered set that can be modified."""

    def update(self, *iterables: Iterable[HashableT]) -> None:  # noqa: D102
        for iterable in iterables:
            for item in iterable:
                self._set_as_dict[item] = None

    @override
    def add(self, value: HashableT) -> None:
        self._set_as_dict[value] = None

    @override
    def discard(self, value: HashableT) -> None:
        self._set_as_dict.pop(value, None)

    @override
    def as_mutable(self) -> MutableOrderedSet[HashableT]:
        return self

    @override
    def as_frozen(self) -> FrozenOrderedSet[HashableT]:
        return FrozenOrderedSet(self)

    @override
    def union(self, other: Iterable[HashableT]) -> Self:  # noqa: D102
        return super().union(other)

    def copy(self) -> MutableOrderedSet[HashableT]:
        """Return a copy of this set."""
        return MutableOrderedSet(iterable=None, _set_as_dict=self._set_as_dict.copy())
