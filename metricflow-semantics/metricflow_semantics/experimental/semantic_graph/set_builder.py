from __future__ import annotations

import logging
from collections.abc import Set
from typing import Generic, Iterable, Optional

from metricflow_semantics.collection_helpers.mf_type_aliases import T
from metricflow_semantics.experimental.ordered_set import MutableOrderedSet, OrderedSet

logger = logging.getLogger(__name__)


class OrderedIntersection(Generic[T]):
    def __init__(self) -> None:
        self._current_set: Optional[MutableOrderedSet[T]] = None

    def intersect(self, set_to_intersect: Set[T]) -> None:
        if self._current_set is None:
            self._current_set = MutableOrderedSet(set_to_intersect)
        else:
            self._current_set.intersection_update(set_to_intersect)

    def result(self) -> OrderedSet[T]:
        if self._current_set is None:
            return MutableOrderedSet()
        return self._current_set

    @staticmethod
    def intersect_sets(sets_to_intersect: Iterable[Set[T]]) -> OrderedSet[T]:
        builder = OrderedIntersection[T]()
        for set_to_intersect in sets_to_intersect:
            builder.intersect(set_to_intersect)
        return builder.result()


class OrderedUnion(Generic[T]):
    def __init__(self) -> None:
        self._current_set: Optional[MutableOrderedSet[T]] = None

    def add(self, set_to_intersect: Set[T]) -> None:
        if self._current_set is None:
            self._current_set = MutableOrderedSet(set_to_intersect)
        else:
            self._current_set.update(set_to_intersect)

    def union(self) -> OrderedSet[T]:
        if self._current_set is None:
            return MutableOrderedSet()
        return self._current_set

    @staticmethod
    def union_sets(sets_to_union: Iterable[Set[T]]) -> OrderedSet[T]:
        builder = OrderedUnion[T]()
        for set_to_intersect in sets_to_union:
            builder.add(set_to_intersect)
        return builder.union()
