from __future__ import annotations

from copy import deepcopy

from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)

# Note: Using `assert results.errors == ()` instead of `assert not results.has_blocking_issues` as the diff shows up
# better in pytest output.


def test_semantic_manifest_validator_default_success(  # noqa: D103
    simple_semantic_manifest: PydanticSemanticManifest,
) -> None:
    semantic_manifest = deepcopy(simple_semantic_manifest)
    validator = SemanticManifestValidator[PydanticSemanticManifest]()
    results = validator.validate_semantic_manifest(semantic_manifest)
    assert not results.has_blocking_issues


def test_semantic_manifest_validator_default_failure(  # noqa: D103
    simple_semantic_manifest: PydanticSemanticManifest,
) -> None:
    semantic_manifest = deepcopy(simple_semantic_manifest)
    semantic_manifest.metrics = []
    semantic_manifest.semantic_models = []

    validator = SemanticManifestValidator[PydanticSemanticManifest]()
    results = validator.validate_semantic_manifest(semantic_manifest)
    assert results.has_blocking_issues


def test_semantic_manifest_validator_succeeds_with_empty_measures(  # noqa: D103
    simple_semantic_manifest: PydanticSemanticManifest,
) -> None:
    semantic_manifest = deepcopy(simple_semantic_manifest)

    validator = SemanticManifestValidator[PydanticSemanticManifest]()
    model = next(model for model in semantic_manifest.semantic_models if model.name == "no_measures_source")

    validator.checked_validations(semantic_manifest)
    assert model.measures == []


def test_multi_process_validator_results_same_as_sync(  # noqa: D103
    simple_semantic_manifest: PydanticSemanticManifest,
) -> None:
    semantic_manifest = deepcopy(simple_semantic_manifest)

    validator = SemanticManifestValidator[PydanticSemanticManifest]()
    default_results = validator.validate_semantic_manifest(semantic_manifest)
    multi_process_results = validator.validate_semantic_manifest(
        semantic_manifest=semantic_manifest, multi_process=True
    )
    assert not default_results.has_blocking_issues
    assert not multi_process_results.has_blocking_issues
    assert default_results.all_issues == multi_process_results.all_issues

    semantic_manifest.metrics = []
    semantic_manifest.semantic_models = []
    default_results = validator.validate_semantic_manifest(semantic_manifest)
    multi_process_results = validator.validate_semantic_manifest(
        semantic_manifest=semantic_manifest, multi_process=True
    )
    assert default_results.has_blocking_issues
    assert multi_process_results.has_blocking_issues
    assert default_results.all_issues == multi_process_results.all_issues
