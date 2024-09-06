from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Callable, Type, TypeVar, Tuple, Dict
from weakref import WeakKeyDictionary

from metricflow_semantics.mf_logging.runtime import log_block_runtime

T = TypeVar("T")


def memoize_hash(cls: Type[T]) -> Type[T]:
    original_init_method: Callable = cls.__init__
    original_hash_method: Callable[[T], int] = cls.__hash__
    # hash_method_to_hash_value: Dict[int, int] = {}
    cache: WeakKeyDictionary[T, int] = WeakKeyDictionary()
    hash_cached = set()

    def replacement_init(self, *args, **kwargs) -> None:
        original_init_method(self, *args, **kwargs)
        cache[self] = original_hash_method(self)
        hash_cached.add(1)

    def replacement_hash_method(self: T) -> int:
        if len(hash_cached) == 0:
            return original_hash_method(self)
        return cache[self]

    cls.__init__ = replacement_init
    cls.__hash__ = replacement_hash_method  # type: ignore[assignment]
    return cls


@memoize_hash
@dataclass(frozen=True)
class StringTuple:
    strings: Tuple[str, ...]

    @cached_property
    def foo(self) -> int:
        return 1


def test_memoize_hash() -> None:
    strings = [str(i) for i in range(100000)]

    st = StringTuple(tuple(strings))
    hash(st)
    with log_block_runtime("hashing"):
        for i in range(10000):
            hash(st)
