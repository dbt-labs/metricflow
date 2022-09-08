import pytest

from metricflow.dataset.dataset import DataSet
from metricflow.model.model_validator import ModelValidator
from metricflow.model.validations.materializations import ValidMaterializationRule
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.test.model.validations.helpers import materialization_with_guaranteed_meta
from metricflow.test.test_utils import model_with_materialization


def test_can_configure_model_validator_rules(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa: D
    model = model_with_materialization(
        simple_model__pre_transforms,
        [
            materialization_with_guaranteed_meta(
                name="foobar",
                metrics=["invalid_bookings"],
                dimensions=[DataSet.metric_time_dimension_name()],
            )
        ],
    )

    # confirm that with the default configuration, an issue is raised
    issues = ModelValidator().validate_model(model).issues
    assert len(issues.all_issues) == 1, f"ModelValidator with default rules had unexpected number of issues {issues}"

    # confirm that a custom configuration excluding ValidMaterializationRule, no issue is raised
    rules = [rule for rule in ModelValidator.DEFAULT_RULES if rule.__class__ is not ValidMaterializationRule]
    issues = ModelValidator(rules=rules).validate_model(model).issues
    assert len(issues.all_issues) == 0, f"ModelValidator without ValidMaterializationRule returned issues {issues}"


def test_cant_configure_model_validator_without_rules() -> None:  # noqa: D
    with pytest.raises(ValueError):
        ModelValidator(rules=[])

    with pytest.raises(ValueError):
        ModelValidator(rules=())

    with pytest.raises(ValueError):
        ModelValidator(rules=None)  # type: ignore
