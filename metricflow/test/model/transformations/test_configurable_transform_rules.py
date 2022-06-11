from metricflow.model.model_transformer import ModelTransformer
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.transformations.transform_rule import ModelTransformRule


class SliceNamesRule(ModelTransformRule):
    """Slice the names of data source elements in a model

    NOTE: specifically for testing
    """

    @staticmethod
    def transform_model(model: UserConfiguredModel) -> UserConfiguredModel:  # noqa: D
        for data_source in model.data_sources:
            data_source.name = data_source.name[:3]
        return model


def test_can_configure_model_transform_rules(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa: D
    pre_model = simple_model__pre_transforms
    assert not all(len(x.name) == 3 for x in pre_model.data_sources)

    # Confirms that a custom transformation works for pre-validation transform
    pre_model = ModelTransformer.pre_validation_transform_model(pre_model, rules=[SliceNamesRule()])
    assert all(len(x.name) == 3 for x in pre_model.data_sources)

    post_model = simple_model__pre_transforms
    assert not all(len(x.name) == 3 for x in post_model.data_sources)

    # Confirms that a custom transformation works for post-validation transform
    post_model = ModelTransformer.post_validation_transform_model(post_model, rules=[SliceNamesRule()])
    assert all(len(x.name) == 3 for x in post_model.data_sources)
