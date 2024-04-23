from __future__ import annotations

import functools
from abc import ABC, abstractmethod
from typing import Iterable, Type, TypeVar

from typing_extensions import Self

MergeableT = TypeVar("MergeableT", bound="Mergeable")


class Mergeable(ABC):
    """Objects that can be merged together to form a superset object of the same type.

    Merging objects are frequently needed in MetricFlow as there are several recursive operations where the output is
    the superset of the result of the recursive calls.

    e.g.
    * The validation issue set of a derived metric includes the issues of the parent metrics.
    * The output of a node in the dataflow plan can include the outputs of the parent nodes.
    * The query-time where filter is useful to combine with the metric-defined where filter.

    Having a common interface also gives a consistent name to this operation so that we don't end up with multiple names
    to describe this operation (e.g. combine, add, concat).

    This is used to streamline the following case that occurs in the codebase:

        items_to_merge: List[ItemType] = []
        ...
        if ...
            items_to_merge.append(...)
        ...
        if ...
            items_to_merge.append(...)
        ...
        if ...
            ...
            items_to_merge.append(...)
            return merge_items(items_to_merge)
        ...
        return merge_items(items_to_merge)

    This centralizes the definition of the merge_items() call.
    """

    @abstractmethod
    def merge(self: Self, other: Self) -> Self:
        """Return a new object that is the result of merging self with other."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def empty_instance(cls: Type[MergeableT]) -> MergeableT:
        """Create an empty instance to handle merging of empty sequences of items.

        As merge_iterable() returns an empty instance for an empty iterable, there needs to be a way of creating one.
        """
        raise NotImplementedError

    @classmethod
    def merge_iterable(cls: Type[MergeableT], items: Iterable[MergeableT]) -> MergeableT:
        """Merge all items into a single instance.

        If an empty iterable has been passed in, this returns an empty instance.
        """
        return functools.reduce(cls.merge, items, cls.empty_instance())
