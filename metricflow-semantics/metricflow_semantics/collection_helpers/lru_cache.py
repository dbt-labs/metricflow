from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Callable, Dict, Generic, Optional, TypeVar

if TYPE_CHECKING:
    # Hack: ensure type checking is not erased for parameters in methods decorated with @lru_cache.
    F = TypeVar("F", bound=Callable)

    def typed_lru_cache(f: F) -> F:  # noqa: D103
        pass

else:
    from functools import lru_cache as typed_lru_cache  # noqa: F401

KeyT = TypeVar("KeyT")
ValueT = TypeVar("ValueT")


class LruCache(Generic[KeyT, ValueT]):
    """An LRU cache based on the insertion order of dictionaries.

    Since Python dictionaries iterate in the order that keys were inserted, they are used as the basis of this cache.
    When an item is retrieved, the item in the dictionary is removed then re-inserted.

    This cache is used instead of the `fuctools.lru_cache` decorator for class instance methods as `lru_cache` keeps a
    reference to the instance, preventing garbage collection of the instance using the decorator until the eviction of
    the associated entry.
    """

    def __init__(self, max_cache_items: int, cache_dict: Optional[Dict[KeyT, ValueT]] = None) -> None:
        """Initializer.

        Args:
            max_cache_items: Limit of cache items to store. Once the limit is hit, the oldest item is evicted.
            cache_dict: For shared use cases - the dictionary to use for the cache.
        """
        self._lock = threading.Lock()
        self._max_cache_items = max_cache_items
        self._cache_dict: Dict[KeyT, ValueT] = cache_dict or {}

    def get(self, key: KeyT) -> Optional[ValueT]:  # noqa: D102
        with self._lock:
            value = self._cache_dict.get(key)
            if value is not None:
                del self._cache_dict[key]
                self._cache_dict[key] = value
                return value

            return None

    def set(self, key: KeyT, value: ValueT) -> None:  # noqa: D102
        with self._lock:
            if key in self._cache_dict:
                return

            while len(self._cache_dict) >= self._max_cache_items:
                key_to_delete = next(iter(self._cache_dict))
                del self._cache_dict[key_to_delete]

            self._cache_dict[key] = value

    def copy(self) -> LruCache:  # noqa: D102
        return LruCache(max_cache_items=self._max_cache_items, cache_dict=dict(self._cache_dict))
