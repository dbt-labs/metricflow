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


def test_invalid_order_by_descending_arg(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)
    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["listings"],
                group_by=["TimeDimension('metric_time')"],
                order_by=["TimeDimension('metric_time').descending(foo)"],
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([SavedQueryRule()])
    check_only_one_error_with_message(
        manifest_validator.validate_semantic_manifest(manifest),
        "An error occurred while parsing a field",
    )


def test_invalid_order_by_due_to_mismatch(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)
    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["listings"],
                group_by=["TimeDimension('metric_time')"],
                order_by=["Dimension('listing__country')"],
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([SavedQueryRule()])
    check_only_one_error_with_message(
        manifest_validator.validate_semantic_manifest(manifest),
        "does not match any of the listed metrics or group-by items.",
    )


def test_order_by_matches_metric(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)
    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["listings"],
                group_by=["TimeDimension('metric_time')"],
                order_by=["Metric('listings').descending(True)"],
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([SavedQueryRule()])
    results = manifest_validator.validate_semantic_manifest(manifest)
    assert results.all_issues == ()


def test_invalid_order_by_due_to_bare_metric(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)
    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["listings"], group_by=["TimeDimension('metric_time')"], order_by=["listings"]
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([SavedQueryRule()])
    check_only_one_error_with_message(
        manifest_validator.validate_semantic_manifest(manifest),
        "An error occurred while parsing a field",
    )
