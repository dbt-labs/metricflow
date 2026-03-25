from __future__ import annotations

from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import PydanticSemanticManifestTransformer
from dbt_semantic_interfaces.transformations.transform_rule import SemanticManifestTransformRule


def modify_manifest(
    semantic_manifest: PydanticSemanticManifest,
    transform_rule: SemanticManifestTransformRule[PydanticSemanticManifest],
) -> PydanticSemanticManifest:
    """Following the given transform, returns a modified copy of the given manifest.

    This should only be used in limited cases as the tests can become hard to read / reason about.
    """
    transformer = PydanticSemanticManifestTransformer()
    transformed_manifest = transformer.transform(semantic_manifest, ((transform_rule,),))

    return transformed_manifest
