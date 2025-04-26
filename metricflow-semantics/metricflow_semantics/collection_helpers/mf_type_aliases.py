from __future__ import annotations

from typing import TypeVar

T1 = TypeVar("T1")
T2 = TypeVar("T2")

# A pair of objects.
Pair = tuple[T1, T2]

# A tuple of any length. Faster to auto-complete than typing out `, ...`
AnyLengthTuple = tuple[T1, ...]

# A tuple of items in a mapping. Each item is a tuple of the form `(key, value)`
# These are useful for type-annotating immutable dataclasses where a mapping is stored as a tuple.
KeyT = TypeVar("KeyT")
ValueT = TypeVar("ValueT")
MappingItem = tuple[KeyT, ValueT]
MappingItemsTuple = tuple[tuple[KeyT, ValueT], ...]
