from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, Iterable, TypeVar

SelfTypeT = TypeVar("SelfTypeT", bound="Mergeable")


class Mergeable(ABC):
    """Collections that can be merged together."""

    @abstractmethod
    def merge(self: SelfTypeT, other: SelfTypeT) -> SelfTypeT:
        """Return a new object that is the result of merging self with other."""
        pass


MergeableT = TypeVar("MergeableT", bound="Mergeable")


class MergeBuilder(Generic[MergeableT]):
    """Implementation of the builder pattern for merging many mergeable objects."""

    def __init__(self, initial_item: MergeableT) -> None:  # noqa: D
        self._accumulator = initial_item

    def add(self, other: MergeableT) -> None:  # noqa: D
        if self._accumulator is None:
            self._accumulator = other
            return

        self._accumulator = self._accumulator.merge(other)

    def add_all(self, *others: MergeableT) -> None:  # noqa: D
        for other in others:
            self.add(other)

    def add_iterable(self, other_iterable: Iterable[MergeableT]) -> None:  # noqa: D
        for other in other_iterable:
            self.add(other)

    @staticmethod
    def merge_iterable(initial_item: MergeableT, other_iterable: Iterable[MergeableT]) -> MergeableT:  # noqa: D
        builder = MergeBuilder[MergeableT](initial_item)
        builder.add_iterable(other_iterable)
        return builder.build_result

    @property
    def build_result(self) -> MergeableT:  # noqa: D
        return self._accumulator
