from __future__ import annotations

import threading
from abc import ABC
from typing import ClassVar, Optional, Type, TypeVar

from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass

SingletonT = TypeVar("SingletonT", bound="Singleton")


@fast_frozen_dataclass()
class Singleton(ABC):
    """Base class for singleton dataclasses.

    * Useful for objects used as keys.
    * Provides significantly faster equality checks / hashing as `id()` can be used (as by Python's default
      object implementations of `__eq__` and `__hash__`).
    * Slower to create due to lookup overhead (see tests).
    * Clunky to use due to the need to implement `get_instance()` manually, and there doesn't seem to be a good way to
      define an abstract method.
    * Pickling not yet supported.
    * Private initializer is simulated through the `_only_init_via_get_instance` field.
    Implementing classes should define a method similar to:

        @classmethod
        def get_instance(cls, ...) -> Self:
            return cls._get_singleton(...)

    """

    # Consider using a `WeakValueDictionary`.
    _instance_dict: ClassVar[dict] = {}
    _instance_dict_lock: ClassVar[threading.RLock] = threading.RLock()

    # Helps to indicate to users that they shouldn't call `__init__`.
    _only_init_via_get_instance: None

    @classmethod
    def _get_instance(cls: Type[SingletonT], **kwargs) -> SingletonT:  # type: ignore[no-untyped-def]
        # Relies on the order of `kwargs` - https://peps.python.org/pep-0468/
        key = (cls, tuple(kwargs.values()))
        instance_dict = cls._instance_dict
        singleton_instance: Optional[SingletonT] = instance_dict.get(key)
        if singleton_instance is not None:
            return singleton_instance

        with cls._instance_dict_lock:
            # Another thread might have created the singleton between the previous check and getting the lock,
            # so check if one exists again.
            singleton_instance = instance_dict.get(key)
            if singleton_instance is None:
                singleton_instance = cls(_only_init_via_get_instance=None, **kwargs)
                instance_dict[key] = singleton_instance
            return singleton_instance
