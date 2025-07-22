from __future__ import annotations

from typing import Any, Hashable, TypeVar

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)

T1 = TypeVar("T1")
T2 = TypeVar("T2")

T1_co = TypeVar("T1_co", covariant=True)
T2_co = TypeVar("T2_co", covariant=True)


HashableT = TypeVar("HashableT", bound=Hashable)
HashableT_co = TypeVar("HashableT_co", bound=Hashable, covariant=True)

# A pair of objects.
Pair = tuple[T1, T2]

# A tuple of any length. Faster to auto-complete than typing out `, ...`
AnyLengthTuple = tuple[T1, ...]

# A tuple of items in a mapping. Each item is a tuple of the form `(key, value)`
# These are useful for type-annotating immutable dataclasses where a mapping is stored as a tuple.
KeyT = TypeVar("KeyT", bound=Hashable)
ValueT = TypeVar("ValueT")
MappingItem = tuple[KeyT, ValueT]
MappingItemsTuple = tuple[tuple[KeyT, ValueT], ...]

ExceptionTracebackAnyType = Any  # type: ignore
