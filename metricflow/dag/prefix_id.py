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

    @override
    def __str__(self) -> str:
        return f"{self.id_prefix.value}_{self.index}"


class PrefixIdGenerator:
    """Generate ID values based on an ID prefix.

    TODO: Migrate ID generation use cases to this class.
    """

    DEFAULT_START_VALUE = 0
    _state_lock = threading.Lock()
    _prefix_to_next_value: Dict[IdPrefix, int] = {}

    @classmethod
    def create_next_id(cls, id_prefix: IdPrefix) -> SequentialId:  # noqa: D
        with cls._state_lock:
            if id_prefix not in cls._prefix_to_next_value:
                cls._prefix_to_next_value[id_prefix] = cls.DEFAULT_START_VALUE
            index = cls._prefix_to_next_value[id_prefix]
            cls._prefix_to_next_value[id_prefix] = index + 1

            return SequentialId(id_prefix, index)
