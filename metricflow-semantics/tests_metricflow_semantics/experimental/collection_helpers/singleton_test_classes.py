"""Classes used in tests that benchmark `Singleton` performance."""
from __future__ import annotations

from collections.abc import Set
from pathlib import Path

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass

PATH_TO_SINGLETON_TEST_CLASS_PY_FILE = Path(__file__)


@fast_frozen_dataclass()
class IdElement:  # noqa: D101
    int_value: int


@fast_frozen_dataclass()
class CompositeId:  # noqa: D101
    id_0: IdElement
    id_1: IdElement


@singleton_dataclass()
class SingletonIdElement:  # noqa: D101
    int_value: int


@singleton_dataclass()
class SingletonCompositeId:  # noqa: D101
    id_0: SingletonIdElement
    id_1: SingletonIdElement


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
        SingletonCompositeId(
            id_0=SingletonIdElement(int_value=i),
            id_1=SingletonIdElement(int_value=i + 1),
        )
        for i in range(size)
    }


FIRST_ID = CompositeId(
    id_0=IdElement(int_value=0),
    id_1=IdElement(int_value=1),
)


FIRST_SINGLETON_ID = SingletonCompositeId(
    id_0=SingletonIdElement(int_value=0),
    id_1=SingletonIdElement(int_value=1),
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
        SingletonCompositeId(
            id_0=SingletonIdElement(int_value=i),
            id_1=SingletonIdElement(int_value=i + 1),
        )
        for i in range(size)
    )
