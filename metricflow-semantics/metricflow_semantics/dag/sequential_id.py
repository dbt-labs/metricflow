from __future__ import annotations

import threading
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from typing import Dict, Generator
from unittest.mock import patch

from typing_extensions import override

from metricflow_semantics.dag.id_prefix import IdPrefix


@dataclass(frozen=True)
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


class SequentialIdGenerator:
    """Generates sequential ID values based on a prefix."""

    _default_start_value = 0
    _state_lock = threading.Lock()
    _prefix_to_next_value: Dict[IdPrefix, int] = {}

    @classmethod
    def create_next_id(cls, id_prefix: IdPrefix) -> SequentialId:  # noqa: D102
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

    @classmethod
    @contextmanager
    def patch_id_generators_helper(cls, start_value: int) -> Generator[None, None, None]:
        """Replace ID generators in IdGeneratorRegistry with one that has the given start value.

        TODO: This method will be modified in a later PR.
        """
        # Create patch context managers for all ID generators in the registry with introspection magic.
        patch_context_managers = [
            patch.object(SequentialIdGenerator, "_prefix_to_next_value", {}),
            patch.object(SequentialIdGenerator, "_default_start_value", start_value),
        ]

        # Enter the patch context for the patches above.
        with ExitStack() as stack:
            for patch_context_manager in patch_context_managers:
                stack.enter_context(patch_context_manager)  # type: ignore
            # This will un-patch when done with the test.
            yield None
