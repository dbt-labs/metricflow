import pytest

from metricflow.model.model_validator import ModelValidator
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import ModelValidationException
from metricflow.test.model.validations.test_unique_valid_name import copied_model
from metricflow.test.test_utils import find_data_source_with


def test_invalid_aggregation_time_dimension(simple_user_configured_model: UserConfiguredModel) -> None:  # noqa:D
    model = copied_model(simple_user_configured_model)
    data_source_with_measures, _ = find_data_source_with(
        model,
        lambda data_source: len(data_source.measures) > 0,
    )

    data_source_with_measures.measures[0].agg_time_dimension = "invalid_time_dimension"

    with pytest.raises(
        ModelValidationException,
        match=(
            "has the aggregation time dimension is set to 'invalid_time_dimension', which not a valid time dimension "
            "in the data source"
        ),
    ):
        model_validator = ModelValidator()
        model_validator.checked_validations(model)
