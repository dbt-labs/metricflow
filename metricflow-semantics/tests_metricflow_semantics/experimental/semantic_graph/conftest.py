from __future__ import annotations

import pytest
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)


@pytest.fixture
def semantic_manifest(
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> PydanticSemanticManifest:
    """Alias for simplified signatures in this module."""
    return simple_semantic_manifest__with_primary_transforms
