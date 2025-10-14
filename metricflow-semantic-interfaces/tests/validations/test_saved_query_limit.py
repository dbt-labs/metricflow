from __future__ import annotations

import copy

from metricflow_semantic_interfaces.implementations.saved_query import (
    PydanticSavedQuery,
    PydanticSavedQueryQueryParams,
)
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.validations.saved_query import SavedQueryRule
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)

from tests.validations.test_saved_query import check_only_one_error_with_message


def test_invalid_limit(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)
    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["listings"],
                limit=-1,
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([SavedQueryRule()])
    check_only_one_error_with_message(
        manifest_validator.validate_semantic_manifest(manifest),
        "Invalid limit value",
    )


def test_valid_limit(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)
    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["listings"],
                limit=1,
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([SavedQueryRule()])
    results = manifest_validator.validate_semantic_manifest(manifest)
    assert results.all_issues == ()


def test_zero_limit(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)
    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["listings"],
                limit=0,
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([SavedQueryRule()])
    results = manifest_validator.validate_semantic_manifest(manifest)
    assert results.all_issues == ()
