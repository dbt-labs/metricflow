from __future__ import annotations

import dataclasses
import logging
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, Generator

from typing_extensions import override

from metricflow_semantics.dag.id_prefix import IdPrefix

logger = logging.getLogger(__name__)


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


@dataclass
class _IdGenerationState:
    """A thread-local class that keeps track of the next IDs to return for a given prefix."""

    default_start_value: int
    prefix_to_next_value: Dict[IdPrefix, int] = dataclasses.field(default_factory=dict)


class _IdGenerationStateStack(threading.local):
    """A thread-local stack that keeps track of the state of ID generation.

    The stack allows the use of context managers to enter sections where ID generation starts at a configured value.
    When entering the section, a new `_IdGenerationState` is pushed on to the stack, and the state at the top of the
    stack is used to generate IDs. When exiting a section, the state is popped off so that ID generation resumes from
    the previous values.

    This stack is thread-local so that ID generation is consistent for a given thread.
    """

    def __init__(self, initial_default_start_value: int = 0) -> None:  # noqa: D107
        self._state_stack = [_IdGenerationState(initial_default_start_value)]

    def push_state(self, id_generation_state: _IdGenerationState) -> None:
        self._state_stack.append(id_generation_state)

    def pop_state(self) -> None:
        state_stack_size = len(self._state_stack)
        if state_stack_size <= 1:
            logger.error(
                f"Attempted to pop the stack when {state_stack_size=}. Since sequential ID generation may not "
                f"be absolutely critical for resolving queries, logging this as an error but it should be "
                f"investigated.",
                stack_info=True,
            )
            return

        self._state_stack.pop(-1)

    @property
    def current_state(self) -> _IdGenerationState:
        return self._state_stack[-1]


class SequentialIdGenerator:
    """Generates sequential ID values based on a prefix."""

    _THREAD_LOCAL_ID_GENERATION_STATE_STACK = _IdGenerationStateStack()

    @classmethod
    def create_next_id(cls, id_prefix: IdPrefix) -> SequentialId:  # noqa: D102
        id_generation_state = cls._THREAD_LOCAL_ID_GENERATION_STATE_STACK.current_state
        if id_prefix not in id_generation_state.prefix_to_next_value:
            id_generation_state.prefix_to_next_value[id_prefix] = id_generation_state.default_start_value
        index = id_generation_state.prefix_to_next_value[id_prefix]
        id_generation_state.prefix_to_next_value[id_prefix] = index + 1
        return SequentialId(id_prefix, index)

    @classmethod
    def reset(cls, default_start_value: int = 0) -> None:
        """Resets the numbering of the generated IDs so that it starts at the given value."""
        id_generation_state = cls._THREAD_LOCAL_ID_GENERATION_STATE_STACK.current_state
        id_generation_state.prefix_to_next_value = {}
        id_generation_state.default_start_value = default_start_value

    @classmethod
    @contextmanager
    def id_number_space(cls, start_value: int) -> Generator[None, None, None]:
        """Open a context where ID generation starts with the given start value.

        On exit, resume ID numbering from prior to entering the context.
        """
        SequentialIdGenerator._THREAD_LOCAL_ID_GENERATION_STATE_STACK.push_state(_IdGenerationState(start_value))
        yield None
        SequentialIdGenerator._THREAD_LOCAL_ID_GENERATION_STATE_STACK.pop_state()
