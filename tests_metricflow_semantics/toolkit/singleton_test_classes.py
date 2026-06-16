"""Classes used in tests that benchmark `Singleton` performance."""
from __future__ import annotations

from collections.abc import Set
from pathlib import Path

from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.singleton import Singleton

PATH_TO_SINGLETON_TEST_CLASS_PY_FILE = Path(__file__)


@fast_frozen_dataclass()
class IdElement:  # noqa: D101
    int_value: int


@fast_frozen_dataclass()
class CompositeId:  # noqa: D101
    id_0: IdElement
    id_1: IdElement


@fast_frozen_dataclass()
class SingletonIdElement(Singleton):  # noqa: D101
    int_value: int

    @classmethod
    def get_instance(cls, int_value: int) -> "SingletonIdElement":  # noqa: D102
        return cls._get_instance(int_value=int_value)


@fast_frozen_dataclass()
class SingletonCompositeId(Singleton):  # noqa: D101
    id_0: SingletonIdElement
    id_1: SingletonIdElement

    @classmethod
    def get_instance(cls, id_0: SingletonIdElement, id_1: SingletonIdElement) -> "SingletonCompositeId":  # noqa: D102
        return cls._get_instance(id_0=id_0, id_1=id_1)


def create_id_set(size: int) -> Set[CompositeId]:  # noqa: D103
    return {
        CompositeId(
            id_0=IdElement(int_value=i),
            id_1=IdElement(int_value=i + 1),
        )
        for i in range(size)
    }


def create_singleton_id_set(size: int) -> Set[SingletonCompositeId]:  # noqa: D103
    return {
        SingletonCompositeId.get_instance(
            id_0=SingletonIdElement.get_instance(int_value=i),
            id_1=SingletonIdElement.get_instance(int_value=i + 1),
        )
        for i in range(size)
    }


FIRST_ID = CompositeId(
    id_0=IdElement(int_value=0),
    id_1=IdElement(int_value=1),
)

FIRST_SINGLETON_ID = SingletonCompositeId.get_instance(
    id_0=SingletonIdElement.get_instance(int_value=0),
    id_1=SingletonIdElement.get_instance(int_value=1),
)


def create_id_tuple(size: int) -> tuple[CompositeId, ...]:  # noqa: D103
    return tuple(
        CompositeId(
            id_0=IdElement(int_value=i),
            id_1=IdElement(int_value=i + 1),
        )
        for i in range(size)
    )


def create_singleton_id_tuple(size: int) -> tuple[SingletonCompositeId, ...]:  # noqa: D103
    return tuple(
        SingletonCompositeId.get_instance(
            id_0=SingletonIdElement.get_instance(int_value=i),
            id_1=SingletonIdElement.get_instance(int_value=i + 1),
        )
        for i in range(size)
    )
