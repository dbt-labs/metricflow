from __future__ import annotations

import logging
from typing import Generic, Optional, TypeVar

from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_type_aliases import ValueT

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class ResultContainer(Generic[ValueT]):
    """Container for a cache result.

    Enables `if result` instead of `if result is not None` to handle falsey result values (e.g. empty set).
    """

    value: ValueT


ResultCacheKeyT = TypeVar("ResultCacheKeyT")


class ResultCache(Generic[ResultCacheKeyT, ValueT]):
    """Cache class to simplify checking / getting / setting the cache for a result.

    Usual pattern:

        result = self._cache.get(cache_key)
        if result is not None:
            return result

        result = compute_result(...)
        self._cache[cache_key] = result
        return result

    Pattern with wrapper:

        result_container = self._cache.get(cache_key)
        return result_container.value if result_container else self._cache.set_and_get(cache_key, compute_result(...))

    There is no lock, so this is intended to be used in cases similar to `cached_property` where the result is equal
    given the cache key. Without the lock, there may be repeated compute, and this shouldn't be used where the caller
    expects the same exact object.

    This is a WIP - there may be easier patterns. This could also be used for instrumenting cache hit rates.
    """

    def __init__(self) -> None:  # noqa: D107
        self._cache_dict: dict[ResultCacheKeyT, ResultContainer[ValueT]] = {}

    def get(self, key: ResultCacheKeyT) -> Optional[ResultContainer[ValueT]]:
        """Returns the cache item for a given key. Also see `ResultContainer`."""
        return self._cache_dict.get(key)

    def set_and_get(self, key: ResultCacheKeyT, value: ValueT) -> ValueT:
        """Set the result for the given key and return the same result.

        This allows the return call to be done in one line.
        """
        self._cache_dict[key] = ResultContainer(value=value)
        return value
