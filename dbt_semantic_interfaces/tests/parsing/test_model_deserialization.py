from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest


def test_model_serialization_deserialization(simple_semantic_manifest: SemanticManifest) -> None:
    """Tests Pydantic serialization and deserialization of a SemanticManifest

    This ensures any custom parsing operations internal to our Pydantic models are properly applied to not only
    user-provided YAML input, but also to internal parsing operations based on serialized model objects.
    """
    serialized_model = simple_semantic_manifest.json()
    deserialized_model = simple_semantic_manifest.parse_raw(serialized_model)
    assert deserialized_model == simple_semantic_manifest
