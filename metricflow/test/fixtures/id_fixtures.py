from __future__ import annotations

from contextlib import ExitStack, contextmanager
from typing import Generator
from unittest.mock import patch

import pytest

from metricflow.dag.id_generation import IdGeneratorRegistry


class IdNumberSpace:
    """Defines the numbering of IDs when setting up tests and test fixtures."""

    # Before each test run.
    TEST_START = 0
    # When setting up the ConsistentIdObjectRepository
    CONSISTENT_ID_REPOSITORY = 10000


@pytest.fixture(autouse=True)
def patch_id_generators() -> Generator[None, None, None]:
    """Patch ID generators with a new one to get repeatability in plan outputs before every test.

    Plan outputs contain IDs, so if the IDs are not consistent from run to run, there will be diffs in the actual vs.
    expected outputs during a test.
    """
    with patch_id_generators_helper(start_value=IdNumberSpace.TEST_START):
        yield None


@contextmanager
def patch_id_generators_helper(start_value: int) -> Generator[None, None, None]:
    """Replace ID generators in IdGeneratorRegistry with one that has the given start value."""
    # Create patch context managers for all ID generators in the registry with introspection magic.
    patch_context_managers = [
        patch.object(IdGeneratorRegistry, "_class_name_to_id_generator", {}),
        patch.object(IdGeneratorRegistry, "DEFAULT_START_VALUE", start_value),
    ]

    # Enter the patch context for the patches above.
    with ExitStack() as stack:
        for patch_context_manager in patch_context_managers:
            stack.enter_context(patch_context_manager)  # type: ignore
        # This will un-patch when done with the test.
        yield None
