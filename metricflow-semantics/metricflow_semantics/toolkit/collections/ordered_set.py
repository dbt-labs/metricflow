from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Hashable, MutableSet, Set
from functools import cached_property
from typing import Generic, Iterable, Iterator, Optional, TypeVar

from typing_extensions import Self, override

from metricflow_semantics.toolkit.comparison_helpers import ComparisonOtherType
from metricflow_semantics.toolkit.mf_type_aliases import HashableT, HashableT_co

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
            _set_as_dict: For internal use cases: Use this exact object for the internal dict. Useful in copy-like
            operations as `dict.copy()` is faster than initializing a new dictionary with items. The performance
            difference is only relevant in tight loops.
        """
        self._set_as_dict = _set_as_dict if _set_as_dict is not None else {}
        if iterable is not None:
            self._set_as_dict.update({item: None for item in iterable})

    def difference(self, *others: Iterable[HashableT_co]) -> Self:  # noqa: D102
        items_to_remove = set(item for other in others for item in other)
        return self.__class__(_set_as_dict={item: None for item in self._set_as_dict if item not in items_to_remove})

    def intersection(self, *others: Iterable[HashableT_co]) -> Self:  # noqa: D102
        common_keys = set(self._set_as_dict).intersection(*others)
        return self.__class__(
            _set_as_dict={
                key: value
                # Iterating through the dict instead of `common_keys` to retain order.
                for key, value in self._set_as_dict.items()
                if key in common_keys
            }
        )

    def union(self, *others: Iterable[HashableT_co]) -> Self:  # noqa: D102
        return self.__class__(_set_as_dict={item: None for other_set in (self,) + tuple(others) for item in other_set})

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

    @abstractmethod
    def copy(self) -> Self:
        """Return a shallow copy of this set."""
        raise NotImplementedError


class FrozenOrderedSet(Generic[HashableT_co], OrderedSet[HashableT_co], Hashable):
    """A frozen version of `OrderedSet` that can be hashed."""

    def __init__(  # noqa: D107
        self,
        iterable: Optional[Iterable[HashableT_co]] = None,
        _set_as_dict: Optional[dict[HashableT_co, None]] = None,
    ) -> None:
        super().__init__(iterable, _set_as_dict)

    @cached_property
    def _cached_hash_value(self) -> int:
        return hash(tuple(self._set_as_dict))

    @override
    def __hash__(self) -> int:
        return self._cached_hash_value

    @override
    def as_mutable(self) -> MutableOrderedSet[HashableT_co]:
        return MutableOrderedSet(_set_as_dict=self._set_as_dict.copy())

    @override
    def as_frozen(self) -> FrozenOrderedSet[HashableT_co]:
        return self

    @override
    def copy(self) -> FrozenOrderedSet[HashableT_co]:
        return FrozenOrderedSet(_set_as_dict=self._set_as_dict)


class MutableOrderedSet(Generic[HashableT], OrderedSet[HashableT], MutableSet[HashableT]):
    """An ordered set that can be modified."""

    def update(self, *iterables: Iterable[HashableT]) -> None:  # noqa: D102
        for iterable in iterables:
            for item in iterable:
                self._set_as_dict[item] = None

    def intersection_update(self, *others: Iterable[HashableT]) -> None:  # noqa: D102
        intersected_items: set[HashableT] = set(self._set_as_dict).intersection(*others)
        self._set_as_dict = {item: None for item in self._set_as_dict if item in intersected_items}

    def difference_update(self, *others: Iterable[HashableT]) -> None:  # noqa: D102
        items_to_remove = {item for other in others for item in other}
        self._set_as_dict = {item: None for item in self._set_as_dict if item not in items_to_remove}

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
        return FrozenOrderedSet(_set_as_dict=self._set_as_dict.copy())

    def clear(self) -> None:
        """Remove all items from this set."""
        self._set_as_dict.clear()

    def pop(self) -> HashableT:
        """Removes and returns the first item in the set. Raises `KeyError` if empty (same as `set()`)."""
        try:
            item = next(iter(self._set_as_dict))
            del self._set_as_dict[item]
            return item
        except StopIteration:
            raise KeyError("Can't pop an item as the set is empty.")

    @override
    def copy(self) -> MutableOrderedSet[HashableT]:
        return MutableOrderedSet(_set_as_dict=self._set_as_dict.copy())
