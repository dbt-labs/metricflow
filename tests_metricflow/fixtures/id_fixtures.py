from __future__ import annotations

from typing import Generator

import pytest
from metricflow_semantics.test_helpers.id_helpers import IdNumberSpace, patch_id_generators_helper


@pytest.fixture(autouse=True, scope="function")
def patch_id_generators() -> Generator[None, None, None]:
    """Patch ID generators with a new one to get repeatability in plan outputs before every test.

    Plan outputs contain IDs, so if the IDs are not consistent from run to run, there will be diffs in the actual vs.
    expected outputs during a test.
    """
    with patch_id_generators_helper(start_value=IdNumberSpace.for_test_start().start_value):
        yield None
