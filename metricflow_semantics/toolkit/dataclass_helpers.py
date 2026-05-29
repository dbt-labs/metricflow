from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from typing import Callable, Type, TypeVar

from typing_extensions import dataclass_transform

_T = TypeVar("_T")

logger = logging.getLogger(__name__)


if sys.version_info >= (3, 10):

    @dataclass_transform(kw_only_default=True, order_default=True, frozen_default=True)
    def fast_frozen_dataclass(order: bool = True) -> Callable[[Type[_T]], Type[_T]]:
        """Decorator for creating an immutable dataclass that is faster than `@dataclass(frozen=True)`.

        * Calling `hash()` on a frozen dataclass always computes the hash from the fields.
        * `hash()` performance can be improved by caching the hash value.
        * A cached hash can help improve lookups times in sets / dicts.
        * This uses a regular, mutable dataclass under the hood, with an update to the hash method.
        * Since MF relies on `mypy`, this uses the `dataclass_transform` decorator to statically check that the code
          does not make mutations.
        * This also improve class instantiation times as there is a performance penalty with creating a frozen dataclass.
        * All fields / recursive fields must be immutable.
        * Includes default arguments for creating dataclasses.

        """

        def _make_dataclass_function(inner_cls: Type[_T]) -> Type[_T]:
            # noinspection PyArgumentList
            cls_to_return = dataclass(  # type: ignore[call-overload]
                inner_cls, kw_only=True, order=order, unsafe_hash=True
            )
            cls_to_return.__mf_previous_hash_method = cls_to_return.__hash__

            def _new_hash_method(self) -> int:  # type: ignore[no-untyped-def]
                # Since the object fields are immutable, not using a lock to reduce overhead.
                cached_hash = self.__mf_cached_hash
                if cached_hash is not None:
                    return cached_hash

                cached_hash = self.__mf_previous_hash_method()
                self.__mf_cached_hash = cached_hash
                return cached_hash

            cls_to_return.__mf_cached_hash = None
            cls_to_return.__hash__ = _new_hash_method

            return inner_cls

        return _make_dataclass_function

else:

    @dataclass_transform(order_default=True, frozen_default=True)
    def fast_frozen_dataclass(order: bool = True) -> Callable[[Type[_T]], Type[_T]]:
        """Similar to above but without `kw_args` as it was added in Python 3.10."""

        def _make_dataclass_function(inner_cls: Type[_T]) -> Type[_T]:
            # noinspection PyArgumentList
            cls_to_return = dataclass(inner_cls, order=order, unsafe_hash=True)  # type: ignore[call-overload]
            cls_to_return.__mf_previous_hash_method = cls_to_return.__hash__

            def _new_hash_method(self) -> int:  # type: ignore[no-untyped-def]
                # Since the object fields are immutable, not using a lock to reduce overhead.
                cached_hash = self.__mf_cached_hash
                if cached_hash is not None:
                    return cached_hash

                cached_hash = self.__mf_previous_hash_method()
                self.__mf_cached_hash = cached_hash
                return cached_hash

            cls_to_return.__mf_cached_hash = None
            cls_to_return.__hash__ = _new_hash_method

            return inner_cls

        return _make_dataclass_function
