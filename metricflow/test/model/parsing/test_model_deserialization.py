from metricflow.model.objects.user_configured_model import UserConfiguredModel


def test_model_serialization_deserialization(simple_user_configured_model: UserConfiguredModel) -> None:
    """Tests Pydantic serialization and deserialization of a UserConfiguredModel

    This ensures any custom parsing operations internal to our Pydantic models are properly applied to not only
    user-provided YAML input, but also to internal parsing operations based on serialized model objects.
    """
    serialized_model = simple_user_configured_model.json()
    deserialized_model = simple_user_configured_model.parse_raw(serialized_model)
    assert deserialized_model == simple_user_configured_model
