from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.references import EntityReference, MeasureReference, MetricReference
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.metric_lookup import MetricLookup
from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import (
    assert_linkable_element_set_snapshot_equal,
    assert_object_snapshot_equal,
)
from metricflow_semantics.time.time_spine_source import TimeSpineSource

logger = logging.getLogger(__name__)


def build_semantic_model_lookup_from_manifest(semantic_manifest: SemanticManifest) -> SemanticModelLookup:  # noqa: D103
    time_spine_sources = TimeSpineSource.build_standard_time_spine_sources(semantic_manifest)
    custom_granularities = TimeSpineSource.build_custom_granularities(list(time_spine_sources.values()))
    return SemanticModelLookup(model=semantic_manifest, custom_granularities=custom_granularities)


@pytest.fixture
def semantic_model_lookup(simple_semantic_manifest: SemanticManifest) -> SemanticModelLookup:  # noqa: D103
    return build_semantic_model_lookup_from_manifest(simple_semantic_manifest)


@pytest.fixture
def multi_hop_semantic_model_lookup(  # noqa: D103
    multi_hop_join_manifest: SemanticManifest,
) -> SemanticModelLookup:
    return build_semantic_model_lookup_from_manifest(multi_hop_join_manifest)


@pytest.fixture
def metric_lookup(  # noqa: D103
    simple_semantic_manifest: SemanticManifest, semantic_model_lookup: SemanticModelLookup
) -> MetricLookup:
    return MetricLookup.create(
        semantic_manifest=simple_semantic_manifest,
        semantic_model_lookup=semantic_model_lookup,
        custom_granularities=semantic_model_lookup.custom_granularities,
    )


@pytest.fixture
def multi_hop_metric_lookup(  # noqa: D103
    multi_hop_join_manifest: SemanticManifest, multi_hop_semantic_model_lookup: SemanticModelLookup
) -> MetricLookup:
    return MetricLookup.create(
        semantic_manifest=multi_hop_join_manifest,
        semantic_model_lookup=multi_hop_semantic_model_lookup,
        custom_granularities=multi_hop_semantic_model_lookup.custom_granularities,
    )


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
            "measure_references": sorted([m.element_name for m in semantic_model_lookup.measure_references]),
            "entity_references": sorted([i.element_name for i in semantic_model_lookup.get_entity_references()]),
        },
    )


def test_local_linked_elements_for_metric(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, metric_lookup: MetricLookup
) -> None:
    linkable_elements = metric_lookup.linkable_elements_for_metrics(
        (MetricReference(element_name="listings"),),
        LinkableElementFilter(
            with_any_of=frozenset({LinkableElementProperty.LOCAL_LINKED}),
            without_any_of=frozenset({LinkableElementProperty.DERIVED_TIME_GRANULARITY}),
        ),
    )
    sorted_specs = sorted(linkable_elements.specs, key=lambda x: x.qualified_name)
    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj_id="result0",
        obj=tuple(spec.qualified_name for spec in sorted_specs),
    )


def test_get_semantic_models_for_entity(semantic_model_lookup: SemanticModelLookup) -> None:  # noqa: D103
    entity_reference = EntityReference(element_name="user")
    linked_semantic_models = semantic_model_lookup.get_semantic_models_for_entity(entity_reference=entity_reference)
    assert len(linked_semantic_models) == 10


def test_linkable_elements_for_metrics(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, metric_lookup: MetricLookup
) -> None:
    assert_linkable_element_set_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        set_id="result0",
        linkable_element_set=metric_lookup.linkable_elements_for_metrics(
            (MetricReference(element_name="views"),),
            LinkableElementFilter(
                without_any_of=frozenset(
                    {
                        LinkableElementProperty.DERIVED_TIME_GRANULARITY,
                        LinkableElementProperty.METRIC_TIME,
                    }
                )
            ),
        ),
    )


def test_linkable_elements_for_measure(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    metric_lookup: MetricLookup,
) -> None:
    """Tests extracting linkable elements for a given measure input."""
    assert_linkable_element_set_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        set_id="result0",
        linkable_element_set=metric_lookup.linkable_elements_for_measure(
            measure_reference=MeasureReference(element_name="listings"),
        ),
    )


def test_linkable_elements_for_measure_multi_hop_model(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    multi_hop_metric_lookup: MetricLookup,
) -> None:
    """Tests extracting linkable elements for a given measure input."""
    assert_linkable_element_set_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        set_id="result0",
        linkable_element_set=multi_hop_metric_lookup.linkable_elements_for_measure(
            measure_reference=MeasureReference(element_name="txn_count"),
        ),
    )


def test_linkable_elements_for_no_metrics_query(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    metric_lookup: MetricLookup,
) -> None:
    """Tests extracting linkable elements for a dimension values query with no metrics."""
    linkable_elements = metric_lookup.linkable_elements_for_no_metrics_query(
        LinkableElementFilter(without_any_of=frozenset({LinkableElementProperty.DERIVED_TIME_GRANULARITY}))
    )
    sorted_specs = sorted(linkable_elements.specs, key=lambda x: x.qualified_name)
    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj_id="result0",
        obj=tuple(spec.qualified_name for spec in sorted_specs),
    )


def test_linkable_set_for_common_dimensions_in_different_models(
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, metric_lookup: MetricLookup
) -> None:
    """Tests case where a metric has dimensions with the same path.

    In this example, "ds" is defined in both "bookings_source" and "views_source".
    """
    assert_linkable_element_set_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        set_id="result0",
        linkable_element_set=metric_lookup.linkable_elements_for_metrics(
            (MetricReference(element_name="bookings_per_view"),),
            LinkableElementFilter(
                without_any_of=frozenset(
                    {
                        LinkableElementProperty.DERIVED_TIME_GRANULARITY,
                    }
                ),
            ),
        ),
    )


def test_get_valid_agg_time_dimensions_for_metric(  # noqa: D103
    metric_lookup: MetricLookup, semantic_model_lookup: SemanticModelLookup
) -> None:
    for metric_name in ["views", "listings", "bookings_per_view"]:
        metric_reference = MetricReference(metric_name)
        metric = metric_lookup.get_metric(metric_reference)
        metric_agg_time_dims = metric_lookup.get_valid_agg_time_dimensions_for_metric(metric_reference)
        measure_agg_time_dims = list(
            {
                semantic_model_lookup.measure_lookup.get_properties(
                    measure.measure_reference
                ).agg_time_dimension_reference
                for measure in metric.input_measures
            }
        )
        if len(measure_agg_time_dims) == 1:
            for metric_agg_time_dim in metric_agg_time_dims:
                assert metric_agg_time_dim.reference == measure_agg_time_dims[0]
        else:
            assert len(metric_agg_time_dims) == 0
