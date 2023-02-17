from metricflow.model.model_transformer import ModelTransformer
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.transformations.transform_rule import ModelTransformRule


class SliceNamesRule(ModelTransformRule):
    """Slice the names of entity elements in a model

    NOTE: specifically for testing
    """

    @staticmethod
    def transform_model(model: UserConfiguredModel) -> UserConfiguredModel:  # noqa: D
        for entity in model.entities:
            entity.name = entity.name[:3]
        return model


def test_can_configure_model_transform_rules(  # noqa: D
    simple_model__with_primary_transforms: UserConfiguredModel,
) -> None:
    pre_model = simple_model__with_primary_transforms
    assert not all(len(x.name) == 3 for x in pre_model.entities)

    # Confirms that a custom transformation works `for ModelTransformer.transform`
    rules = [SliceNamesRule()]
    transformed_model = ModelTransformer.transform(pre_model, ordered_rule_sequences=(rules,))
    assert all(len(x.name) == 3 for x in transformed_model.entities)
