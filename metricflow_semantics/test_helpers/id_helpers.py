from __future__ import annotations

from dataclasses import dataclass
from typing import Generator

import pytest

from metricflow_semantics.dag.sequential_id import SequentialIdGenerator


@pytest.fixture(autouse=True, scope="function")
def setup_id_generators() -> Generator[None, None, None]:
    """Setup ID generation to start numbering at a specific value to get repeatability in generated IDs.

    Plan outputs contain IDs, so if the IDs are not consistent from run to run, there will be diffs in the actual vs.
    expected outputs during a test.

    Fixtures may generate IDs, so this needs to be done before every test.
    """
    with SequentialIdGenerator.id_number_space(start_value=IdNumberSpace.for_test_start().start_value):
        yield None


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
