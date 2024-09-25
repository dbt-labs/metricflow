from __future__ import annotations

import pytest
from dbt_semantic_interfaces.implementations.project_configuration import PydanticProjectConfiguration
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.references import (
    SemanticModelReference,
)
from metricflow_semantics.api.v0_1.saved_query_dependency_resolver import SavedQueryDependencyResolver


@pytest.fixture(scope="session")
def resolver(  # noqa: D103
    simple_semantic_manifest: PydanticSemanticManifest,
) -> SavedQueryDependencyResolver:
    return SavedQueryDependencyResolver(simple_semantic_manifest)


def test_saved_query_dependency_resolver(resolver: SavedQueryDependencyResolver) -> None:  # noqa: D103
    dependency_set = resolver.resolve_dependencies("p0_booking")
    assert tuple(dependency_set.semantic_model_references) == (
        SemanticModelReference(semantic_model_name="bookings_source"),
        SemanticModelReference(semantic_model_name="listings_latest"),
    )


def test_empty_manifest() -> None:  # noqa: D103
    """In case the manifest is empty, checks that the resolver can be created."""
    SavedQueryDependencyResolver(
        PydanticSemanticManifest(
            semantic_models=[],
            metrics=[],
            project_configuration=PydanticProjectConfiguration(),
        )
    )
