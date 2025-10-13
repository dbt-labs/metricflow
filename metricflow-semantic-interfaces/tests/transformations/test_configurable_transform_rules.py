from __future__ import annotations

from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.transformations.semantic_manifest_transformer import (
    PydanticSemanticManifestTransformer,
)
from metricflow_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)


class SliceNamesRule(SemanticManifestTransformRule):
    """Slice the names of semantic model elements in a model.

    NOTE: specifically for testing
    """

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D102
        for semantic_model in semantic_manifest.semantic_models:
            semantic_model.name = semantic_model.name[:3]
        return semantic_manifest


def test_can_configure_model_transform_rules(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    pre_model = simple_semantic_manifest__with_primary_transforms
    assert not all(len(x.name) == 3 for x in pre_model.semantic_models)

    # Confirms that a custom transformation works `for ModelTransformer.transform`
    rules = [SliceNamesRule()]
    transformed_model = PydanticSemanticManifestTransformer.transform(pre_model, ordered_rule_sequences=(rules,))
    assert all(len(x.name) == 3 for x in transformed_model.semantic_models)
