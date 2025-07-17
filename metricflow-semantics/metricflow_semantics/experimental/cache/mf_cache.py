from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Callable, Generic, Optional
from weakref import WeakValueDictionary

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple, KeyT, ValueT

logger = logging.getLogger(__name__)


class MetricflowCache(ABC):
    pass


class CompositeCache(MetricflowCache, ABC):
    @property
    @abstractmethod
    def caches(self) -> AnyLengthTuple[MetricflowCache]:
        raise NotImplementedError()


class WeakValueDictCache(Generic[KeyT, ValueT], MetricflowCache):
    def __init__(self) -> None:
        self._cache: WeakValueDictionary[KeyT, ValueT] = WeakValueDictionary()

    def get(self, cache_key: KeyT) -> Optional[ValueT]:
        return self._cache.get(cache_key)

    def get_or_create(self, cache_key: KeyT, factory: Callable[[], ValueT]) -> ValueT:
        value = self._cache.get(cache_key)
        if value is None:
            value = factory()
            self._cache[cache_key] = value
        return value
