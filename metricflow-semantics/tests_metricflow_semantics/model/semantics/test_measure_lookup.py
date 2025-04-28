from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.references import MeasureReference
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantics.measure_lookup import MeasureLookup
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_object_snapshot_equal


@pytest.fixture(scope="module")
def measure_lookup(extended_date_semantic_manifest_lookup: SemanticManifestLookup) -> MeasureLookup:  # noqa: D103
    return extended_date_semantic_manifest_lookup.semantic_model_lookup.measure_lookup


def test_get_measure(extended_date_manifest: PydanticSemanticManifest, measure_lookup: MeasureLookup) -> None:
    """Test that all measures in the manifest can be retrieved."""
    for semantic_model in extended_date_manifest.semantic_models:
        for measure in semantic_model.measures:
            assert measure == measure_lookup.get_measure(measure.reference)


def test_measure_properties(
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, measure_lookup: MeasureLookup
) -> None:
    """Test a couple of measures for correct properties."""
    # Check `bookings` and `booking_payments` together as they have different aggregation time dimensions.
    measure_names = ["bookings", "bookings_monthly"]
    result = {
        measure_name: measure_lookup.get_properties(MeasureReference(measure_name)) for measure_name in measure_names
    }
    assert_object_snapshot_equal(
        request=request, snapshot_configuration=mf_test_configuration, obj_id="obj_0", obj=result
    )
