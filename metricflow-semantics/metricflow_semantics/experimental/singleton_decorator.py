from __future__ import annotations

import dataclasses
import logging
import sys
import threading
import types
import typing
from typing import Any, Callable, Type, TypeVar

from typing_extensions import dataclass_transform

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

if typing.TYPE_CHECKING:
    from _typeshed import DataclassInstance

    SingletonInstance = TypeVar("SingletonInstance", bound=DataclassInstance)
else:
    SingletonInstance = TypeVar("SingletonInstance")

logger = logging.getLogger(__name__)


# `kw_only_default` is supported in Python >= 3.10
if sys.version_info >= (3, 10):

    @dataclass_transform(kw_only_default=True, order_default=True, frozen_default=True)
    def singleton_dataclass(order: bool = True) -> Callable[[Type[SingletonInstance]], Type[SingletonInstance]]:
        """Helper method to get a decorator for defining singleton dataclasses.

        Example:
            @singleton_dataclass()
            class ColorId:
                int_value: int

            assert ColorId(int_value=1) is ColorId(int_value=1)

        * Similar to `fast_frozen_dataclass` - see notes there.
        * Default values for fields are not yet supported to make it easier to generate the lookup key.
        * Seems like there should be something more standard to replace this.
        """

        def _wrapper(inner_cls: Type[SingletonInstance]) -> Type[SingletonInstance]:
            return _recreate_dataclass_as_singleton(fast_frozen_dataclass(order=order)(inner_cls))

        return _wrapper

else:

    @dataclass_transform(order_default=True, frozen_default=True)
    def singleton_dataclass(order: bool = True) -> Callable[[Type[SingletonInstance]], Type[SingletonInstance]]:
        """Similar to above but for older Python versions."""

        def _wrapper(inner_cls: Type[DataclassInstance]) -> Type[SingletonInstance]:
            return _recreate_dataclass_as_singleton(fast_frozen_dataclass(order=order)(inner_cls))

        return _wrapper


def _recreate_dataclass_as_singleton(inner_cls: Type[DataclassInstance]) -> Type[SingletonInstance]:
    """Given a dataclass, create a similar one with a different metaclass so that it's a singleton."""
    class_to_return = inner_cls

    # Check for fields with default field values.
    for dataclass_field in dataclasses.fields(class_to_return):
        if (
            dataclass_field.default is not dataclasses.MISSING
            or dataclass_field.default_factory is not dataclasses.MISSING
        ):
            raise ValueError(
                LazyFormat(
                    "Default values for fields are currently not supported to simplify field-value resolution.",
                    class_name=class_to_return.__name__,
                    dataclass_field=dataclass_field,
                )
            )

    instance_lock = threading.Lock()
    # Might change to  `WeakValueDictionary`.
    instance_args_to_instance: dict[AnyLengthTuple, SingletonInstance] = {}

    dataclass_metaclass = type(class_to_return)

    class SingletonMeta(dataclass_metaclass):  # type: ignore[valid-type, misc]
        """Class-specific metaclass to return an existing instance if one exists."""

        def __call__(self, *args, **kwargs):  # type: ignore
            key = tuple(args) + tuple(sorted(kwargs.items()))
            instance = instance_args_to_instance.get(key)
            if instance is not None:
                return instance
            with instance_lock:
                instance = instance_args_to_instance.get(key)
                if instance is not None:
                    return instance

                instance = super().__call__(*args, **kwargs)
                instance_args_to_instance[key] = instance
                return instance

    def exec_body(namespace: dict[str, Any]) -> None:  # type: ignore[misc]
        for name, obj in class_to_return.__dict__.items():
            if name not in {
                # These will be handled by Python.
                "__dict__",
                "__weakref__",
                "__doc__",
                "__module__",
            }:
                namespace[name] = obj

    return types.new_class(  # type: ignore
        name=class_to_return.__name__,
        bases=class_to_return.__bases__,
        kwds={"metaclass": SingletonMeta},
        exec_body=exec_body,
    )
