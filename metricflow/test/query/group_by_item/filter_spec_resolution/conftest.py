from __future__ import annotations

from typing import Sequence

import pytest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest

from metricflow.test.query.group_by_item.filter_spec_resolution.ambiguous_filter_resolution_test_case_builder import (
    AmbiguousFilterResolutionTestCase,
    AmbiguousFilterResolutionTestCaseBuilder,
)


@pytest.fixture(scope="session")
def ambiguous_filter_query_cases(
    ambiguous_resolution_manifest: PydanticSemanticManifest,
) -> Sequence[AmbiguousFilterResolutionTestCase]:
    """Returns a generated list of cases to test resolution of ambiguous group-by-items in filters.

    See AmbiguousFilterResolutionTestCaseBuilder for the permutations.
    """
    test_case_builder = AmbiguousFilterResolutionTestCaseBuilder(
        ambiguous_resolution_manifest=ambiguous_resolution_manifest
    )
    return test_case_builder.build_all_test_configurations()
