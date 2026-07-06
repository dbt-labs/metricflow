from __future__ import annotations

import logging

import pytest
from metricflow_semantics.toolkit.cache.weighted_lru_result_cache import WeightedLruResultCache
from metricflow_semantics.toolkit.mf_type_aliases import ValueT

logger = logging.getLogger(__name__)


def _assert_cached_value(
    cache: WeightedLruResultCache[str, ValueT],
    key: str,
    expected_value: ValueT,
    expected_weight: int,
) -> None:
    cache_entry = cache.get(key)

    assert cache_entry is not None
    assert cache_entry.value == expected_value
    assert cache_entry.weight == expected_weight


def test_set_and_get_caches_entry() -> None:
    """The cache entry can be retrieved after the value is set."""
    cache = WeightedLruResultCache[str, object](weight_limit=1)
    value = object()

    assert cache.set_and_get("key", value, weight=1) is value

    _assert_cached_value(cache, "key", value, expected_weight=1)


def test_lru_eviction() -> None:
    """The LRU entry is selected by access order while eviction capacity uses the entry weight."""
    cache = WeightedLruResultCache[str, str](weight_limit=5)

    cache.set_and_get("key_0", "value_0", weight=3)
    cache.set_and_get("key_1", "value_1", weight=1)

    _assert_cached_value(cache, "key_0", "value_0", expected_weight=3)

    cache.set_and_get("key_2", "value_2", weight=2)

    assert cache.get("key_1") is None
    _assert_cached_value(cache, "key_0", "value_0", expected_weight=3)
    _assert_cached_value(cache, "key_2", "value_2", expected_weight=2)


def test_oversized_entry_is_not_cached() -> None:
    """An entry larger than the cache weight limit is returned but not stored."""
    cache = WeightedLruResultCache[str, str](weight_limit=1)

    assert cache.set_and_get("key", "value", weight=2) == "value"

    assert cache.get("key") is None


def test_negative_entry_weight_raises_error() -> None:
    """Entry weight must be non-negative."""
    cache = WeightedLruResultCache[str, str](weight_limit=1)

    with pytest.raises(ValueError, match="Cache entry weight should be >= 0"):
        cache.set_and_get("key", "value", weight=-1)


def test_negative_weight_limit_raises_error() -> None:
    """Cache weight limit must be non-negative."""
    with pytest.raises(ValueError, match="Weight limit should be >= 0"):
        WeightedLruResultCache[str, str](weight_limit=-1)
