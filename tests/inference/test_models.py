from __future__ import annotations

from metricflow.inference.models import InferenceSignalType


def test_inference_type_node_conflict() -> None:
    """Make sure the inference signal type hierarchy is correctly configured."""
    # IDENTIFIER
    assert InferenceSignalType.ID.UNKNOWN.is_subtype_of(InferenceSignalType.UNKNOWN)
    assert not InferenceSignalType.ID.UNKNOWN.is_subtype_of(InferenceSignalType.DIMENSION.UNKNOWN)
    assert not InferenceSignalType.ID.UNKNOWN.is_subtype_of(InferenceSignalType.MEASURE.UNKNOWN)

    assert InferenceSignalType.ID.UNIQUE.is_subtype_of(InferenceSignalType.ID.UNKNOWN)
    assert InferenceSignalType.ID.FOREIGN.is_subtype_of(InferenceSignalType.ID.UNKNOWN)
    assert InferenceSignalType.ID.PRIMARY.is_subtype_of(InferenceSignalType.ID.UNIQUE)

    # DIMENSION
    assert InferenceSignalType.DIMENSION.UNKNOWN.is_subtype_of(InferenceSignalType.UNKNOWN)
    assert not InferenceSignalType.DIMENSION.UNKNOWN.is_subtype_of(InferenceSignalType.ID.UNKNOWN)
    assert not InferenceSignalType.DIMENSION.UNKNOWN.is_subtype_of(InferenceSignalType.MEASURE.UNKNOWN)

    assert InferenceSignalType.DIMENSION.CATEGORICAL.is_subtype_of(InferenceSignalType.DIMENSION.UNKNOWN)
    assert InferenceSignalType.DIMENSION.TIME.is_subtype_of(InferenceSignalType.DIMENSION.UNKNOWN)

    # MEASURE
    assert InferenceSignalType.MEASURE.UNKNOWN.is_subtype_of(InferenceSignalType.UNKNOWN)
    assert not InferenceSignalType.MEASURE.UNKNOWN.is_subtype_of(InferenceSignalType.ID.UNKNOWN)
    assert not InferenceSignalType.MEASURE.UNKNOWN.is_subtype_of(InferenceSignalType.DIMENSION.UNKNOWN)
