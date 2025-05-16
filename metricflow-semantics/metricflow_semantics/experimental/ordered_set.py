from __future__ import annotations

import logging
from abc import ABC
from collections.abc import Hashable, MutableSet, Set
from functools import cached_property
from typing import Generic, Iterable, Iterator, Optional, TypeVar

from typing_extensions import override

from metricflow_semantics.experimental.comparison_helpers import ComparisonOtherType

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=Hashable)
OrderedSetT = TypeVar("OrderedSetT", bound="OrderedSet")


class OrderedSet(Generic[T], Set[T], ABC):
    """Set where the iteration order is the insertion order.

    * Having a consistent iteration order is helpful for ensuring consistency in tests and snapshot generation without
      sorting.
    * Since this is meant to be used for consistency in iteration, eq / hash behavior does not depend on order.
    * Considering using the `ordered-set` package instead, but it would add a dependency and this implementation is
      short.
    """

    def __init__(self, iterable: Optional[Iterable[T]] = None) -> None:  # noqa: D107
        if iterable is None:
            self._set_as_dict = {}
        else:
            self._set_as_dict = {item: None for item in iterable}

    @classmethod
    def create_from_items(cls: type[OrderedSetT], *items: T) -> OrderedSetT:
        """Create a set with the arguments as set items.

        e.g. `create_from_items(1, 2, 3) -> {1, 2, 3}`
        """
        return cls(items)

    @classmethod
    def create_from_iterables(cls: type[OrderedSetT], *iterables: Iterable[T]) -> OrderedSetT:  # noqa: D102
        return cls(item for iterable in iterables for item in iterable)

    def intersection(self: OrderedSetT, other: Iterable[T]) -> OrderedSetT:  # noqa: D102
        other_set = frozenset(other)
        return self.__class__(item for item in self if item in other_set)

    def union(self: OrderedSetT, other: Iterable[T]) -> OrderedSetT:  # noqa: D102
        return self.__class__(tuple(self._set_as_dict.keys()) + tuple(other))

    @override
    def __contains__(self, obj: ComparisonOtherType) -> bool:
        return obj in self._set_as_dict

    @override
    def __len__(self) -> int:
        return len(self._set_as_dict)

    @override
    def __iter__(self) -> Iterator[T]:
        return iter(self._set_as_dict)

    @override
    def __str__(self) -> str:
        return "{" + ", ".join(str(item) for item in self) + "}"


class FrozenOrderedSet(Generic[T], OrderedSet[T], Hashable):
    """A frozen version of `OrderedSet` that can be hashed."""

    @cached_property
    def _cached_hash_value(self) -> int:
        return hash(frozenset(self._set_as_dict.keys()))

    @override
    def __hash__(self) -> int:
        return self._cached_hash_value


class MutableOrderedSet(Generic[T], OrderedSet[T], MutableSet[T]):
    """An ordered set that can be modified."""

    def update(self, *iterables: Iterable[T]) -> None:  # noqa: D102
        for iterable in iterables:
            for item in iterable:
                self._set_as_dict[item] = None

    @override
    def add(self, value: T) -> None:
        self._set_as_dict[value] = None

    @override
    def discard(self, value: T) -> None:
        self._set_as_dict.pop(value, None)
