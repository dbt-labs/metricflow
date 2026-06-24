from __future__ import annotations

import logging
import threading
from typing import Generic, Optional

from metricflow_semantics.toolkit.cache.result_cache import ResultCacheKeyT, ValueT
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class WeightedLruResultCacheEntry(Generic[ValueT]):
    """Container for a cache result and its eviction weight."""

    value: ValueT
    weight: int


class WeightedLruResultCache(Generic[ResultCacheKeyT, ValueT]):
    """A result cache that evicts least-recently-used entries to stay under a total weight limit."""

    def __init__(self, weight_limit: int) -> None:  # noqa: D107
        if weight_limit < 0:
            raise ValueError(LazyFormat("Weight limit should be >= 0", weight_limit=weight_limit))

        self._weight_limit = weight_limit
        self._current_weight = 0
        self._cache_dict: dict[ResultCacheKeyT, WeightedLruResultCacheEntry[ValueT]] = {}
        self._lock = threading.Lock()

    def get(self, key: ResultCacheKeyT) -> Optional[WeightedLruResultCacheEntry[ValueT]]:
        """Returns the cache entry for a given key."""
        with self._lock:
            cache_entry = self._cache_dict.get(key)
            if cache_entry is None:
                return None

            del self._cache_dict[key]
            self._cache_dict[key] = cache_entry
            return cache_entry

    def set_and_get(self, key: ResultCacheKeyT, value: ValueT, weight: int) -> ValueT:
        """Set the result for the given key and return the same result.

        If the total weight of the entries in the cache exceeds the threshold, the least recently used items are evicted
        to make room for the new item.
        """
        if weight < 0:
            raise ValueError(LazyFormat("Cache entry weight should be >= 0", weight=weight))

        with self._lock:
            previous_cache_entry = self._cache_dict.pop(key, None)
            if previous_cache_entry is not None:
                self._current_weight -= previous_cache_entry.weight

            new_cache_entry = WeightedLruResultCacheEntry(value=value, weight=weight)
            if new_cache_entry.weight > self._weight_limit:
                return value

            while self._current_weight + new_cache_entry.weight > self._weight_limit:
                lru_key = next(iter(self._cache_dict))
                lru_cache_entry = self._cache_dict.pop(lru_key)
                self._current_weight -= lru_cache_entry.weight

            self._cache_dict[key] = new_cache_entry
            self._current_weight += new_cache_entry.weight

        return value
