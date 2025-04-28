from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Set
from typing import Any, Generic, Iterable, Iterator, Mapping, Optional, TypeVar

from typing_extensions import Self, override

from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.mf_logging.pretty_formatter import PrettyFormatContext

logger = logging.getLogger(__name__)

T = TypeVar("T")


class OrderedSet(MetricFlowPrettyFormattable, Generic[T], Set[T], ABC):
    @abstractmethod
    def intersection(self, other: Set[T]) -> Self:
        raise NotImplementedError

    @abstractmethod
    def union(self, other: Set[T]) -> Self:
        raise NotImplementedError

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return format_context.formatter.pretty_format({item for item in self})


BaseOrderedSetT = TypeVar("BaseOrderedSetT", bound="BaseOrderedSet")


# noinspection PyUnresolvedReferences
class BaseOrderedSet(Generic[T], OrderedSet[T]):
    def __init__(self, item_mapping: Mapping[T, None]) -> None:
        self._set_as_dict = dict(item_mapping)

    @classmethod
    def create_from_args(cls: type[BaseOrderedSetT], *args: T) -> BaseOrderedSetT:
        return cls({arg: None for arg in args})

    @classmethod
    def create_from_iterable(cls: type[BaseOrderedSetT], items: Iterable[T]) -> BaseOrderedSetT:
        return cls({item: None for item in items})

    @override
    def intersection(self: BaseOrderedSetT, other: Set[T]) -> BaseOrderedSetT:
        common_items = set(self._set_as_dict.keys()).intersection(other)
        return self.__class__.create_from_iterable(common_items)

    @override
    def union(self: BaseOrderedSetT, other: Set[T]) -> BaseOrderedSetT:
        union_items = set(self._set_as_dict.keys()).union(other)
        return self.__class__.create_from_iterable(union_items)

    @override
    def __contains__(self, obj: Any) -> bool:  # type: ignore[misc]
        return self._set_as_dict.__contains__(obj)

    @override
    def __len__(self) -> int:
        return len(self._set_as_dict)

    @override
    def __iter__(self) -> Iterator[T]:
        return iter(self._set_as_dict)


class FrozenOrderedSet(Generic[T], BaseOrderedSet[T]):
    pass


class MutableOrderedSet(BaseOrderedSet[T]):
    def add(self, item: T) -> None:
        self._set_as_dict[item] = None

    def add_iterables(self, *iterables: Iterable[T]) -> None:
        for iterable in iterables:
            for item in iterable:
                self.add(item)

    def frozen_copy(self) -> FrozenOrderedSet[T]:
        return FrozenOrderedSet(self._set_as_dict)
