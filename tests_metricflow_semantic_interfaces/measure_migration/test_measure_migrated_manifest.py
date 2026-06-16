from __future__ import annotations

from copy import deepcopy

from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)


def test_successful_validation(
    measure_migrated_manifest: PydanticSemanticManifest,
) -> None:
    """Test that the manifest with measures migrated to simple metrics passes validation."""
    semantic_manifest = deepcopy(measure_migrated_manifest)
    validator = SemanticManifestValidator[PydanticSemanticManifest]()
    results = validator.validate_semantic_manifest(semantic_manifest)
    assert not results.has_blocking_issues
