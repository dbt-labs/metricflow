from __future__ import annotations

from metricflow_semantics.toolkit.cache.lru_cache import LruCache


def test_lru_cache() -> None:  # noqa: D103
    cache = LruCache[str, str](max_cache_items=2)
    cache.set("key_0", "value_0")
    cache.set("key_1", "value_1")
    cache.set("key_2", "value_2")

    # This should evict "key_0".
    assert cache.get("key_0") is None

    # Get "key_1" so that it's not evicted next.
    assert cache.get("key_1") == "value_1"

    # This should evict "key_2".
    cache.set("key_0", "value_0")
    assert cache.get("key_2") is None
