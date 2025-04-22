from __future__ import annotations

import threading
from abc import ABC
from typing import ClassVar, Optional, Type, TypeVar

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass

SingletonT = TypeVar("SingletonT", bound="Singleton")


@fast_frozen_dataclass()
class Singleton(ABC):
    """Base class for singletons.

    * Provides significantly faster equality checks / hashing as `id()` can be used (as used in Python's default
      object implementations of `__eq__` and `__hash__`).
    * Slower to create due to lookup overhead (see tests).
    * May be useful for ID-like objects.
    * Clunky to use due to the need to implement `get_instance()` manually, and there doesn't seem to be a good way to
      define an abstract method.
    * Pickling not yet supported.
    * Private initializer is simulated through the `_created_via_singleton` field.

    Implementing classes should define a method like:

        @classmethod
        def get_instance(cls, ...) -> Self:
            return cls._get_singleton_by_kwargs(...)
    """

    # This might be better as a weakref.
    _instance_dict: ClassVar[dict] = {}
    _instance_dict_lock: ClassVar[threading.Lock] = threading.Lock()

    # Helps to indicate to uses that they shouldn't call `__init__`, but doesn't actually do anything.
    _created_via_singleton: bool

    @classmethod
    def _get_singleton_by_kwargs(  # type: ignore[no-untyped-def]
        cls: Type[SingletonT],
        **kwargs,
    ) -> SingletonT:
        key = tuple(kwargs.values())
        matching_singleton: Optional[SingletonT] = cls._instance_dict.get(key)
        if matching_singleton is not None:
            return matching_singleton

        with cls._instance_dict_lock:
            matching_singleton = cls._instance_dict.get(key)
            if matching_singleton is None:
                matching_singleton = cls(_created_via_singleton=True, **kwargs)
                cls._instance_dict[key] = matching_singleton
            return matching_singleton
