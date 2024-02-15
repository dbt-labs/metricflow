from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Dict

from typing_extensions import override

from metricflow.dag.id_prefix import IdPrefix


@dataclass(frozen=True)
class SequentialId:
    """Returns a sequentially numbered ID based on a prefix."""

    id_prefix: IdPrefix
    index: int

    @property
    def str_value(self) -> str:  # noqa: D
        return f"{self.id_prefix.str_value}_{self.index}"

    @override
    def __repr__(self) -> str:
        return self.str_value


class SequentialIdGenerator:
    """Generates sequential ID values based on a prefix."""

    _default_start_value = 0
    _state_lock = threading.Lock()
    _prefix_to_next_value: Dict[IdPrefix, int] = {}

    @classmethod
    def create_next_id(cls, id_prefix: IdPrefix) -> SequentialId:  # noqa: D
        with cls._state_lock:
            if id_prefix not in cls._prefix_to_next_value:
                cls._prefix_to_next_value[id_prefix] = cls._default_start_value
            index = cls._prefix_to_next_value[id_prefix]
            cls._prefix_to_next_value[id_prefix] = index + 1

            return SequentialId(id_prefix, index)

    @classmethod
    def reset(cls, default_start_value: int = 0) -> None:
        """Resets the numbering of the generated IDs so that it starts at the given value."""
        with cls._state_lock:
            cls._prefix_to_next_value = {}
            cls._default_start_value = default_start_value
