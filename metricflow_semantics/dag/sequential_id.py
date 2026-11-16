from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import ClassVar, Generator

from metricflow_semantics.dag.id_prefix import IdPrefix
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from typing_extensions import override

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class SequentialId:
    """Returns a sequentially numbered ID based on a prefix."""

    id_prefix: IdPrefix
    index: int

    @property
    def str_value(self) -> str:  # noqa: D102
        return f"{self.id_prefix.str_value}_{self.index}"

    @override
    def __repr__(self) -> str:
        return self.str_value


class _IdGenerationState(threading.local):
    """Stores the mapping from the IdPrefix to the next index in a thread-local manner."""

    def __init__(self, default_start_value: int) -> None:
        self.default_start_value = default_start_value
        self.id_prefix_to_next_value: dict[IdPrefix, int] = {}

    def reset(self, default_start_value: int) -> None:
        """Reset ID generation for the current thread."""
        self.default_start_value = default_start_value
        self.id_prefix_to_next_value = {}

    def restore(self, default_start_value: int, id_prefix_to_next_value: dict[IdPrefix, int]) -> None:
        """Restore ID generation state for the current thread."""
        self.default_start_value = default_start_value
        self.id_prefix_to_next_value = id_prefix_to_next_value


class SequentialIdGenerator:
    """Generates sequential ID values based on a prefix."""

    _id_generation_state: ClassVar[_IdGenerationState] = _IdGenerationState(0)

    @classmethod
    def create_next_id(cls, id_prefix: IdPrefix) -> SequentialId:  # noqa: D102
        id_generation_state = cls._id_generation_state
        id_prefix_to_next_value = id_generation_state.id_prefix_to_next_value
        next_index = id_prefix_to_next_value.get(id_prefix, id_generation_state.default_start_value)
        id_prefix_to_next_value[id_prefix] = next_index + 1

        return SequentialId(id_prefix=id_prefix, index=next_index)

    @classmethod
    def reset(cls, default_start_value: int = 0) -> None:
        """Resets the numbering of the generated IDs so that it starts at the given value."""
        cls._id_generation_state.reset(default_start_value)

    @classmethod
    @contextmanager
    def id_number_space(cls, start_value: int) -> Generator[None, None, None]:
        """Open a context where ID generation starts with the given start value.

        On exit, resume ID numbering from prior to entering the context.
        """
        id_generation_state = cls._id_generation_state
        previous_default_start_value = id_generation_state.default_start_value
        previous_id_prefix_to_next_value = id_generation_state.id_prefix_to_next_value
        id_generation_state.reset(start_value)
        try:
            yield None
        finally:
            id_generation_state.restore(previous_default_start_value, previous_id_prefix_to_next_value)
