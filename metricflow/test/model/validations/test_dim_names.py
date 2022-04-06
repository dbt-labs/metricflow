from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.dim_names import DimensionAndIdentifierNameValidator


def test_dimension_name_validator(simple_model__pre_transforms: UserConfiguredModel) -> None:  # noqa: D
    dim_name_validator = DimensionAndIdentifierNameValidator(simple_model__pre_transforms)

    # Check a local dimension.
    assert dim_name_validator.is_dimension_valid_for_metric(metric_name="bookings", dimension_name="is_instant")

    # Check a dimension in a different data source that's specified like a local dimension.
    assert not dim_name_validator.is_dimension_valid_for_metric(metric_name="bookings", dimension_name="country_latest")

    # Check dimension in a different data source that can be joined in.
    assert dim_name_validator.is_dimension_valid_for_metric(
        metric_name="bookings", dimension_name="listing__country_latest"
    )

    # Check dimension in a different data source that can't be joined in.
    assert not dim_name_validator.is_dimension_valid_for_metric(
        metric_name="bookings", dimension_name="verification__verification_type"
    )
