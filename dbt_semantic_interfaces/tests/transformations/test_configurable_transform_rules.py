from dbt_semantic_interfaces.model_transformer import ModelTransformer
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.transformations.transform_rule import ModelTransformRule


class SliceNamesRule(ModelTransformRule):
    """Slice the names of semantic model elements in a model

    NOTE: specifically for testing
    """

    @staticmethod
    def transform_model(model: SemanticManifest) -> SemanticManifest:  # noqa: D
        for semantic_model in model.semantic_models:
            semantic_model.name = semantic_model.name[:3]
        return model


def test_can_configure_model_transform_rules(  # noqa: D
    simple_model__with_primary_transforms: SemanticManifest,
) -> None:
    pre_model = simple_model__with_primary_transforms
    assert not all(len(x.name) == 3 for x in pre_model.semantic_models)

    # Confirms that a custom transformation works `for ModelTransformer.transform`
    rules = [SliceNamesRule()]
    transformed_model = ModelTransformer.transform(pre_model, ordered_rule_sequences=(rules,))
    assert all(len(x.name) == 3 for x in transformed_model.semantic_models)
