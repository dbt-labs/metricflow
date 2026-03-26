from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantics.model.semantics.dimension_lookup import DimensionLookup
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_object_snapshot_equal


@pytest.fixture(scope="module")
def dimension_lookup(  # noqa: D103
    partitioned_multi_hop_join_semantic_manifest: PydanticSemanticManifest,
) -> DimensionLookup:
    return DimensionLookup(partitioned_multi_hop_join_semantic_manifest.semantic_models)


def test_get_invariant(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    partitioned_multi_hop_join_semantic_manifest: PydanticSemanticManifest,
    dimension_lookup: DimensionLookup,
) -> None:
    """Test invariants for all dimensions.

    Uses `partitioned_multi_hop_join_semantic_manifest` to show an example of different `is_partition` values.
    """
    dimension_references = []
    for semantic_model in partitioned_multi_hop_join_semantic_manifest.semantic_models:
        for dimension in semantic_model.dimensions:
            dimension_references.append(dimension.reference)

    sorted_dimension_references = sorted(dimension_references)
    result = {
        dimension_reference.element_name: dimension_lookup.get_invariant(dimension_reference)
        for dimension_reference in sorted_dimension_references
    }
    assert_object_snapshot_equal(
        request=request, snapshot_configuration=mf_test_configuration, obj_id="obj_0", obj=result
    )
