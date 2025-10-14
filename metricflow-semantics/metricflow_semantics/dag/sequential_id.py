from __future__ import annotations

import logging
from contextlib import contextmanager
from contextvars import ContextVar
from functools import cached_property
from typing import Generator, Mapping, Optional

from typing_extensions import override

from metricflow_semantics.dag.id_prefix import IdPrefix
from metricflow_semantics.toolkit.collections.mapping_helpers import mf_items_to_dict, mf_mapping_to_items
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple, MappingItem

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


@fast_frozen_dataclass()
class _IdGenerationState:
    """Keeps track of the next IDs to return for a given prefix.

    Since this is used in a context variable, it can't be mutable or else mutations will be visible between contexts.
    """

    default_start_value: int
    prefix_to_next_value_items: AnyLengthTuple[MappingItem[IdPrefix, int]]

    @cached_property
    def prefix_to_next_value(self) -> Mapping[IdPrefix, int]:
        return mf_items_to_dict(self.prefix_to_next_value_items)

    @staticmethod
    def create(
        default_start_value: int, prefix_to_next_value: Optional[Mapping[IdPrefix, int]] = None
    ) -> _IdGenerationState:
        return _IdGenerationState(
            default_start_value=default_start_value,
            prefix_to_next_value_items=mf_mapping_to_items(prefix_to_next_value or {}),
        )


class _IdGenerationStateStack:
    """A stack that keeps track of the state of ID generation.

    The stack allows the use of context managers to enter sections where ID generation starts at a configured value.
    When entering the section, a new `_IdGenerationState` is pushed on to the stack, and the state at the top of the
    stack is used to generate IDs. When exiting a section, the state is popped off so that ID generation resumes from
    the previous values.

    The bottom of the stack is the one at the lowest index in `_state_stack_items`, and the top of the stack is the highest.

    The "current state" is the state at the top of the stack.
    """

    _state_stack_items: ContextVar[AnyLengthTuple[_IdGenerationState]] = ContextVar(
        "_IdGenerationStateStack__state_stack_items"
    )

    @classmethod
    def _get_stack_items(cls) -> AnyLengthTuple[_IdGenerationState]:
        if cls._state_stack_items.get(None) is None:
            cls._state_stack_items.set((_IdGenerationState.create(default_start_value=0),))
        return cls._state_stack_items.get()

    @classmethod
    def push_state(cls, id_generation_state: _IdGenerationState) -> None:
        cls._state_stack_items.set(cls._get_stack_items() + (id_generation_state,))

    @classmethod
    def pop_state(cls) -> None:
        initial_items = cls._get_stack_items()
        state_stack_size = len(initial_items)
        if state_stack_size <= 1:
            logger.error(
                LazyFormat(
                    "Attempted to pop the last element in the state stack. Since sequential ID generation may not "
                    "be absolutely critical for resolving queries, logging this as an error but it should be "
                    "investigated.",
                    state_stack_size=state_stack_size,
                ),
                stack_info=True,
            )
            return

        cls._state_stack_items.set(initial_items[:-1])

    @classmethod
    def get_current_state(cls) -> _IdGenerationState:
        return cls._get_stack_items()[-1]

    @classmethod
    def replace_current_state(cls, updated_state: _IdGenerationState) -> None:
        """Remove the current state and replace it the new state."""
        cls._state_stack_items.set(cls._get_stack_items()[:-1] + (updated_state,))


class SequentialIdGenerator:
    """Generates sequential ID values based on a prefix."""

    @classmethod
    def create_next_id(cls, id_prefix: IdPrefix) -> SequentialId:  # noqa: D102
        id_generation_state = _IdGenerationStateStack.get_current_state()
        next_index = id_generation_state.prefix_to_next_value.get(id_prefix) or id_generation_state.default_start_value
        updated_state = _IdGenerationState.create(
            default_start_value=id_generation_state.default_start_value,
            prefix_to_next_value={**id_generation_state.prefix_to_next_value, **{id_prefix: next_index + 1}},
        )
        _IdGenerationStateStack.replace_current_state(updated_state)

        return SequentialId(id_prefix=id_prefix, index=next_index)

    @classmethod
    def reset(cls, default_start_value: int = 0) -> None:
        """Resets the numbering of the generated IDs so that it starts at the given value."""
        _IdGenerationStateStack.replace_current_state(
            _IdGenerationState.create(default_start_value=default_start_value)
        )

    @classmethod
    @contextmanager
    def id_number_space(cls, start_value: int) -> Generator[None, None, None]:
        """Open a context where ID generation starts with the given start value.

        On exit, resume ID numbering from prior to entering the context.
        """
        _IdGenerationStateStack.push_state(_IdGenerationState.create(default_start_value=start_value))
        yield None
        _IdGenerationStateStack.pop_state()
