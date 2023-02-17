import pytest

from metricflow.dataset.dataset import DataSet
from metricflow.model.model_validator import ModelValidator
from metricflow.model.objects.user_configured_model import UserConfiguredModel

def test_cant_configure_model_validator_without_rules() -> None:  # noqa: D
    with pytest.raises(ValueError):
        ModelValidator(rules=[])

    with pytest.raises(ValueError):
        ModelValidator(rules=())

    with pytest.raises(ValueError):
        ModelValidator(rules=None)  # type: ignore
