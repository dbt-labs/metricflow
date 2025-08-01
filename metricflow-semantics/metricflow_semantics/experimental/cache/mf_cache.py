from __future__ import annotations

import logging
from functools import cached_property
from typing import Generic, Optional, TypeVar

from metricflow_semantics.collection_helpers.mf_type_aliases import ValueT

logger = logging.getLogger(__name__)


class CachedResult(Generic[ValueT]):
    """Container for a cache result.

    Enables `if result` instead of `if result is not None` for falsey result values.
    """

    def __init__(self, value: ValueT) -> None:  # noqa: D107
        self._value = value

    @cached_property
    def value(self) -> ValueT:  # noqa: D102
        return self._value


ResultCacheKeyT = TypeVar("ResultCacheKeyT")


class ResultCache(Generic[ResultCacheKeyT, ValueT]):
    """Cache class to simplify checking / getting / setting the cache for a result.

    Usual pattern:

        result = self._cache.get(cache_key)
        if result is not None:
            return result

        result = compute_result(...)
        self._cache[cache_key] = result
        return result.

    Pattern with wrapper:

        result = self._cache.get(cache_key)
        return result.value if result else self._cache.set_and_get(cache_key, compute_result(...))

    This is a WIP - there may be easier patterns. This could also be used for instrumenting cache hit rates.
    """

    def __init__(self) -> None:  # noqa: D107
        self._cache_dict: dict[ResultCacheKeyT, CachedResult[ValueT]] = {}

    def get(self, key: ResultCacheKeyT) -> Optional[CachedResult[ValueT]]:
        """Returns the cache item for a given key. Also see `CachedResult`."""
        return self._cache_dict.get(key)

    def set_and_get(self, key: ResultCacheKeyT, value: ValueT) -> ValueT:
        """Set the result for the given key and return the same result.

        This allows the return call to be done in one line.
        """
        self._cache_dict[key] = CachedResult(value)
        return value
