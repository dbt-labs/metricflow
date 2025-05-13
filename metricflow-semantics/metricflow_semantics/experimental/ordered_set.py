from __future__ import annotations

import logging
from abc import ABC
from collections.abc import Set
from typing import Any, Generic, Iterable, Iterator, Mapping, Optional, TypeVar

from typing_extensions import override

from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.mf_logging.pretty_formatter import PrettyFormatContext

logger = logging.getLogger(__name__)

T = TypeVar("T")
OrderedSetT = TypeVar("OrderedSetT", bound="OrderedSet")


# noinspection PyUnresolvedReferences
class OrderedSet(MetricFlowPrettyFormattable, Generic[T], Set[T], ABC):
    """Abstract set where the iteration order is the insertion order.

    Having a consistent iteration order is helpful for ensuring consistency in tests and snapshot generations without
    sorting.
    """

    def __init__(self, item_mapping: Mapping[T, None]) -> None:
        """Initializer."""
        self._set_as_dict = dict(item_mapping)

    @classmethod
    def create_from_args(cls: type[OrderedSetT], *args: T) -> OrderedSetT:
        """Create a set with the arguments as set items.

        e.g. `create_from_args(1, 2, 3) -> {1, 2, 3}``
        """
        return cls({arg: None for arg in args})

    @classmethod
    def create_from_iterable(cls: type[OrderedSetT], items: Iterable[T]) -> OrderedSetT:  # noqa: D102
        return cls({item: None for item in items})

    def intersection(self: OrderedSetT, other: Set[T]) -> OrderedSetT:  # noqa: D102
        common_items = set(self._set_as_dict.keys()).intersection(other)
        return self.__class__.create_from_iterable(common_items)

    def union(self: OrderedSetT, other: Set[T]) -> OrderedSetT:  # noqa: D102
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

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return format_context.formatter.pretty_format({item for item in self})


class FrozenOrderedSet(Generic[T], OrderedSet[T]):
    """An ordered set that can't be modified."""

    pass


class MutableOrderedSet(OrderedSet[T]):
    """An ordered set that can be modified."""

    def add(self, item: T) -> None:  # noqa: D102
        self._set_as_dict[item] = None

    def add_iterables(self, *iterables: Iterable[T]) -> None:  # noqa: D102
        for iterable in iterables:
            for item in iterable:
                self.add(item)

    def as_frozen_set(self) -> FrozenOrderedSet[T]:  # noqa: D102
        return FrozenOrderedSet(self._set_as_dict)
