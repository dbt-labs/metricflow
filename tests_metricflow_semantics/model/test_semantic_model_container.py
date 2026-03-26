from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.references import EntityReference, MetricReference
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantics.metric_lookup import MetricLookup
from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import (
    assert_object_snapshot_equal,
)
from metricflow_semantics.time.time_spine_source import TimeSpineSource

logger = logging.getLogger(__name__)


def build_semantic_model_lookup_from_manifest(semantic_manifest: SemanticManifest) -> SemanticModelLookup:  # noqa: D103
    time_spine_sources = TimeSpineSource.build_standard_time_spine_sources(semantic_manifest)
    custom_granularities = TimeSpineSource.build_custom_granularities(list(time_spine_sources.values()))
    return SemanticModelLookup(semantic_manifest=semantic_manifest, custom_granularities=custom_granularities)


@pytest.fixture
def semantic_model_lookup(simple_semantic_manifest: SemanticManifest) -> SemanticModelLookup:  # noqa: D103
    return build_semantic_model_lookup_from_manifest(simple_semantic_manifest)


@pytest.fixture
def multi_hop_semantic_model_lookup(  # noqa: D103
    multi_hop_join_manifest: SemanticManifest,
) -> SemanticModelLookup:
    return build_semantic_model_lookup_from_manifest(multi_hop_join_manifest)


@pytest.fixture
def metric_lookup(simple_semantic_manifest: SemanticManifest) -> MetricLookup:  # noqa: D103
    return SemanticManifestLookup(simple_semantic_manifest).metric_lookup


@pytest.fixture
def multi_hop_metric_lookup(multi_hop_join_manifest: SemanticManifest) -> MetricLookup:  # noqa: D103
    return SemanticManifestLookup(multi_hop_join_manifest).metric_lookup


def test_get_names(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    semantic_model_lookup: SemanticModelLookup,
) -> None:
    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj_id="result0",
        obj={
            "dimension_references": sorted([d.element_name for d in semantic_model_lookup.get_dimension_references()]),
            "entity_references": sorted([i.element_name for i in semantic_model_lookup.get_entity_references()]),
        },
    )


def test_get_semantic_models_for_entity(semantic_model_lookup: SemanticModelLookup) -> None:  # noqa: D103
    entity_reference = EntityReference(element_name="user")
    linked_semantic_models = semantic_model_lookup.get_semantic_models_for_entity(entity_reference=entity_reference)
    assert len(linked_semantic_models) == 10


def test_get_aggregation_time_dimension_specs(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    metric_lookup: MetricLookup,
    semantic_model_lookup: SemanticModelLookup,
) -> None:
    result = {}
    for metric_name in ["views", "listings", "bookings_per_view"]:
        specs = metric_lookup.get_aggregation_time_dimension_specs(MetricReference(metric_name))
        result[metric_name] = list(spec.dunder_name for spec in specs)

    assert_object_snapshot_equal(request=request, snapshot_configuration=mf_test_configuration, obj=result)
