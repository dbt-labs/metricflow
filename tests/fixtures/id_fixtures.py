from __future__ import annotations

from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from typing import Generator
from unittest.mock import patch

import pytest

from metricflow.dag.sequential_id import SequentialIdGenerator


@dataclass(frozen=True)
class IdNumberSpace:
    """Defines the numbering of IDs when setting up tests and test fixtures."""

    start_value: int

    @staticmethod
    def for_test_start() -> IdNumberSpace:
        """Before each test run."""
        return IdNumberSpace(0)

    @staticmethod
    def for_block(block_index: int, block_size: int = 2000, first_block_offset: int = 10000) -> IdNumberSpace:
        """Used to define fixed-size blocks of ID number spaces.

        This is useful for creating unique / non-overlapping IDs for different groups of objects.
        """
        if not block_index >= 0:
            raise RuntimeError(f"block_index should be >= 0. Got: {block_index}")
        if not block_size > 1:
            raise RuntimeError(f"block_size should be > 1. Got: {block_size}")
        if not first_block_offset >= 0:
            raise RuntimeError(f"first_block_offset should be >= 0. Got: {first_block_offset}")
        return IdNumberSpace(first_block_offset + block_size * block_index)


@pytest.fixture(autouse=True, scope="function")
def patch_id_generators() -> Generator[None, None, None]:
    """Patch ID generators with a new one to get repeatability in plan outputs before every test.

    Plan outputs contain IDs, so if the IDs are not consistent from run to run, there will be diffs in the actual vs.
    expected outputs during a test.
    """
    with patch_id_generators_helper(start_value=IdNumberSpace.for_test_start().start_value):
        yield None


@contextmanager
def patch_id_generators_helper(start_value: int) -> Generator[None, None, None]:
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
