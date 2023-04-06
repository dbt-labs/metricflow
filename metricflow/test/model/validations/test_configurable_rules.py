import pytest

from metricflow.model.model_validator import ModelValidator


def test_cant_configure_model_validator_without_rules() -> None:  # noqa: D
    with pytest.raises(ValueError):
        ModelValidator(rules=[])

    with pytest.raises(ValueError):
        ModelValidator(rules=())

    with pytest.raises(ValueError):
        ModelValidator(rules=None)  # type: ignore
