from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.references import EntityReference, MeasureReference, MetricReference

from metricflow.model.semantics.linkable_element_properties import LinkableElementProperties
from metricflow.model.semantics.metric_lookup import MetricLookup
from metricflow.model.semantics.semantic_model_lookup import SemanticModelLookup
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.snapshot_utils import assert_linkable_element_set_snapshot_equal, assert_object_snapshot_equal

logger = logging.getLogger(__name__)


@pytest.fixture
def semantic_model_lookup(simple_semantic_manifest: SemanticManifest) -> SemanticModelLookup:  # Noqa: D
    return SemanticModelLookup(
        model=simple_semantic_manifest,
    )


@pytest.fixture
def metric_lookup(  # Noqa: D
    simple_semantic_manifest: SemanticManifest, semantic_model_lookup: SemanticModelLookup
) -> MetricLookup:
    return MetricLookup(semantic_manifest=simple_semantic_manifest, semantic_model_lookup=semantic_model_lookup)


def test_get_names(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    semantic_model_lookup: SemanticModelLookup,
) -> None:
    assert_object_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        obj_id="result0",
        obj={
            "dimension_references": sorted([d.element_name for d in semantic_model_lookup.get_dimension_references()]),
            "measure_references": sorted([m.element_name for m in semantic_model_lookup.measure_references]),
            "entity_references": sorted([i.element_name for i in semantic_model_lookup.get_entity_references()]),
        },
    )


def test_get_elements(semantic_model_lookup: SemanticModelLookup) -> None:  # noqa: D
    for dimension_reference in semantic_model_lookup.get_dimension_references():
        assert (
            semantic_model_lookup.get_dimension(dimension_reference=dimension_reference).reference
            == dimension_reference
        )
    for measure_reference in semantic_model_lookup.measure_references:
        measure_reference = MeasureReference(element_name=measure_reference.element_name)
        assert semantic_model_lookup.get_measure(measure_reference=measure_reference).reference == measure_reference


def test_get_semantic_models_for_measure(semantic_model_lookup: SemanticModelLookup) -> None:  # noqa: D
    bookings_sources = semantic_model_lookup.get_semantic_models_for_measure(MeasureReference(element_name="bookings"))
    assert len(bookings_sources) == 1
    assert bookings_sources[0].name == "bookings_source"

    views_sources = semantic_model_lookup.get_semantic_models_for_measure(MeasureReference(element_name="views"))
    assert len(views_sources) == 1
    assert views_sources[0].name == "views_source"

    listings_sources = semantic_model_lookup.get_semantic_models_for_measure(MeasureReference(element_name="listings"))
    assert len(listings_sources) == 1
    assert listings_sources[0].name == "listings_latest"


def test_elements_for_metric(  # noqa: D
    request: FixtureRequest, mf_test_session_state: MetricFlowTestSessionState, metric_lookup: MetricLookup
) -> None:
    assert_object_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        obj_id="result0",
        obj=tuple(
            spec.qualified_name
            for spec in metric_lookup.element_specs_for_metrics(
                [MetricReference(element_name="views")],
                without_any_property=frozenset(
                    {
                        LinkableElementProperties.DERIVED_TIME_GRANULARITY,
                        LinkableElementProperties.METRIC_TIME,
                    }
                ),
            )
        ),
    )


def test_local_linked_elements_for_metric(  # noqa: D
    request: FixtureRequest, mf_test_session_state: MetricFlowTestSessionState, metric_lookup: MetricLookup
) -> None:
    assert_object_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        obj_id="result0",
        obj=tuple(
            spec.qualified_name
            for spec in metric_lookup.element_specs_for_metrics(
                [MetricReference(element_name="listings")],
                with_any_property=frozenset({LinkableElementProperties.LOCAL_LINKED}),
                without_any_property=frozenset({LinkableElementProperties.DERIVED_TIME_GRANULARITY}),
            )
        ),
    )


def test_get_semantic_models_for_entity(semantic_model_lookup: SemanticModelLookup) -> None:  # noqa: D
    entity_reference = EntityReference(element_name="user")
    linked_semantic_models = semantic_model_lookup.get_semantic_models_for_entity(entity_reference=entity_reference)
    assert len(linked_semantic_models) == 8


def test_linkable_set(  # noqa: D
    request: FixtureRequest, mf_test_session_state: MetricFlowTestSessionState, metric_lookup: MetricLookup
) -> None:
    assert_linkable_element_set_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        set_id="result0",
        linkable_element_set=metric_lookup.linkable_set_for_metrics(
            (MetricReference(element_name="views"),),
            without_any_property=frozenset(
                {
                    LinkableElementProperties.DERIVED_TIME_GRANULARITY,
                    LinkableElementProperties.METRIC_TIME,
                }
            ),
        ),
    )


def test_linkable_set_for_common_dimensions_in_different_models(
    request: FixtureRequest, mf_test_session_state: MetricFlowTestSessionState, metric_lookup: MetricLookup
) -> None:
    """Tests case where a metric has dimensions with the same path.

    In this example, "ds" is defined in both "bookings_source" and "views_source".
    """
    assert_linkable_element_set_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        set_id="result0",
        linkable_element_set=metric_lookup.linkable_set_for_metrics(
            (MetricReference(element_name="bookings_per_view"),),
            without_any_property=frozenset(
                {
                    LinkableElementProperties.DERIVED_TIME_GRANULARITY,
                }
            ),
        ),
    )
