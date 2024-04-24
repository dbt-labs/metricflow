from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.references import EntityReference, MeasureReference, MetricReference
from metricflow_semantics.model.semantics.linkable_element import LinkableElementProperty
from metricflow_semantics.model.semantics.metric_lookup import MetricLookup
from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup

from tests_metricflow.fixtures.setup_fixtures import MetricFlowTestConfiguration
from tests_metricflow.snapshot_utils import assert_linkable_element_set_snapshot_equal, assert_object_snapshot_equal

logger = logging.getLogger(__name__)


@pytest.fixture
def semantic_model_lookup(simple_semantic_manifest: SemanticManifest) -> SemanticModelLookup:  # noqa: D103
    return SemanticModelLookup(
        model=simple_semantic_manifest,
    )


@pytest.fixture
def metric_lookup(  # noqa: D103
    simple_semantic_manifest: SemanticManifest, semantic_model_lookup: SemanticModelLookup
) -> MetricLookup:
    return MetricLookup(semantic_manifest=simple_semantic_manifest, semantic_model_lookup=semantic_model_lookup)


def test_get_names(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    semantic_model_lookup: SemanticModelLookup,
) -> None:
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj={
            "dimension_references": sorted([d.element_name for d in semantic_model_lookup.get_dimension_references()]),
            "measure_references": sorted([m.element_name for m in semantic_model_lookup.measure_references]),
            "entity_references": sorted([i.element_name for i in semantic_model_lookup.get_entity_references()]),
        },
    )


def test_get_elements(semantic_model_lookup: SemanticModelLookup) -> None:  # noqa: D103
    for dimension_reference in semantic_model_lookup.get_dimension_references():
        assert (
            semantic_model_lookup.get_dimension(dimension_reference=dimension_reference).reference
            == dimension_reference
        )
    for measure_reference in semantic_model_lookup.measure_references:
        measure_reference = MeasureReference(element_name=measure_reference.element_name)
        assert semantic_model_lookup.get_measure(measure_reference=measure_reference).reference == measure_reference


def test_get_semantic_model_for_measure(semantic_model_lookup: SemanticModelLookup) -> None:  # noqa: D103
    bookings_source = semantic_model_lookup.get_semantic_model_for_measure(MeasureReference(element_name="bookings"))
    assert bookings_source.name == "bookings_source"

    views_source = semantic_model_lookup.get_semantic_model_for_measure(MeasureReference(element_name="views"))
    assert views_source.name == "views_source"

    listings_source = semantic_model_lookup.get_semantic_model_for_measure(MeasureReference(element_name="listings"))
    assert listings_source.name == "listings_latest"


def test_local_linked_elements_for_metric(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, metric_lookup: MetricLookup
) -> None:
    linkable_elements = metric_lookup.linkable_elements_for_metrics(
        [MetricReference(element_name="listings")],
        with_any_property=frozenset({LinkableElementProperty.LOCAL_LINKED}),
        without_any_property=frozenset({LinkableElementProperty.DERIVED_TIME_GRANULARITY}),
    )
    sorted_specs = sorted(linkable_elements.as_spec_set.as_tuple, key=lambda x: x.qualified_name)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
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
        mf_test_configuration=mf_test_configuration,
        set_id="result0",
        linkable_element_set=metric_lookup.linkable_elements_for_metrics(
            (MetricReference(element_name="views"),),
            without_any_property=frozenset(
                {
                    LinkableElementProperty.DERIVED_TIME_GRANULARITY,
                    LinkableElementProperty.METRIC_TIME,
                }
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
        mf_test_configuration=mf_test_configuration,
        set_id="result0",
        linkable_element_set=metric_lookup.linkable_elements_for_measure(
            measure_reference=MeasureReference(element_name="listings"),
        ),
    )


def test_linkable_elements_for_no_metrics_query(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    metric_lookup: MetricLookup,
) -> None:
    """Tests extracting linkable elements for a dimension values query with no metrics."""
    linkable_elements = metric_lookup.linkable_elements_for_no_metrics_query(
        without_any_of={
            LinkableElementProperty.DERIVED_TIME_GRANULARITY,
        }
    )
    sorted_specs = sorted(linkable_elements.as_spec_set.as_tuple, key=lambda x: x.qualified_name)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
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
        mf_test_configuration=mf_test_configuration,
        set_id="result0",
        linkable_element_set=metric_lookup.linkable_elements_for_metrics(
            (MetricReference(element_name="bookings_per_view"),),
            without_any_property=frozenset(
                {
                    LinkableElementProperty.DERIVED_TIME_GRANULARITY,
                }
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
                semantic_model_lookup.get_agg_time_dimension_for_measure(measure.measure_reference)
                for measure in metric.input_measures
            }
        )
        if len(measure_agg_time_dims) == 1:
            for metric_agg_time_dim in metric_agg_time_dims:
                assert metric_agg_time_dim.reference == measure_agg_time_dims[0]
        else:
            assert len(metric_agg_time_dims) == 0


def test_get_agg_time_dimension_specs_for_measure(semantic_model_lookup: SemanticModelLookup) -> None:  # noqa: D103
    for measure_name in ["bookings", "views", "listings"]:
        measure_reference = MeasureReference(measure_name)
        agg_time_dim_specs = semantic_model_lookup.get_agg_time_dimension_specs_for_measure(measure_reference)
        agg_time_dim_reference = semantic_model_lookup.get_agg_time_dimension_for_measure(measure_reference)
        for spec in agg_time_dim_specs:
            assert spec.reference == agg_time_dim_reference
