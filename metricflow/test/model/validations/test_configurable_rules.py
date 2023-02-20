import pytest

from metricflow.dataset.dataset import DataSet
from dbt.semantic.validations.model_validator import ModelValidator
from dbt.contracts.graph.manifest import UserConfiguredModel

def test_cant_configure_model_validator_without_rules() -> None:  # noqa: D
    with pytest.raises(ValueError):
        ModelValidator(rules=[])

    with pytest.raises(ValueError):
        ModelValidator(rules=())

    with pytest.raises(ValueError):
        ModelValidator(rules=None)  # type: ignore
