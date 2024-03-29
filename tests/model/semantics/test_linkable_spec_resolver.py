from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.references import EntityReference, MetricReference, SemanticModelReference

from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.model.semantics.linkable_element_properties import LinkableElementProperties
from metricflow.model.semantics.linkable_spec_resolver import (
    SemanticModelJoinPath,
    SemanticModelJoinPathElement,
    ValidLinkableSpecResolver,
)
from metricflow.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS
from tests.fixtures.setup_fixtures import MetricFlowTestConfiguration
from tests.snapshot_utils import assert_linkable_element_set_snapshot_equal

logger = logging.getLogger(__name__)


@pytest.fixture
def simple_model_spec_resolver(  # noqa: D103
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> ValidLinkableSpecResolver:
    return ValidLinkableSpecResolver(
        semantic_manifest=simple_semantic_manifest_lookup.semantic_manifest,
        semantic_model_lookup=simple_semantic_manifest_lookup.semantic_model_lookup,
        max_entity_links=MAX_JOIN_HOPS,
    )


@pytest.fixture
def cyclic_join_manifest_spec_resolver(  # noqa: D103
    cyclic_join_semantic_manifest_lookup: SemanticManifestLookup,
) -> ValidLinkableSpecResolver:
    return ValidLinkableSpecResolver(
        semantic_manifest=cyclic_join_semantic_manifest_lookup.semantic_manifest,
        semantic_model_lookup=cyclic_join_semantic_manifest_lookup.semantic_model_lookup,
        max_entity_links=MAX_JOIN_HOPS,
    )


def test_all_properties(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    simple_model_spec_resolver: ValidLinkableSpecResolver,
) -> None:
    assert_linkable_element_set_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        set_id="result0",
        linkable_element_set=simple_model_spec_resolver.get_linkable_elements_for_metrics(
            metric_references=[MetricReference(element_name="bookings"), MetricReference(element_name="views")],
            with_any_of=LinkableElementProperties.all_properties(),
            without_any_of=frozenset({}),
        ),
    )


def test_one_property(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    simple_model_spec_resolver: ValidLinkableSpecResolver,
) -> None:
    assert_linkable_element_set_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        set_id="result0",
        linkable_element_set=simple_model_spec_resolver.get_linkable_elements_for_metrics(
            metric_references=[MetricReference(element_name="bookings"), MetricReference(element_name="views")],
            with_any_of=frozenset({LinkableElementProperties.LOCAL}),
            without_any_of=frozenset(),
        ),
    )


def test_metric_time_property_for_cumulative_metric(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    simple_model_spec_resolver: ValidLinkableSpecResolver,
) -> None:
    assert_linkable_element_set_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        set_id="result0",
        linkable_element_set=simple_model_spec_resolver.get_linkable_elements_for_metrics(
            metric_references=[MetricReference(element_name="trailing_2_months_revenue")],
            with_any_of=frozenset({LinkableElementProperties.METRIC_TIME}),
            without_any_of=frozenset(),
        ),
    )


def test_metric_time_property_for_derived_metrics(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    simple_model_spec_resolver: ValidLinkableSpecResolver,
) -> None:
    assert_linkable_element_set_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        set_id="result0",
        linkable_element_set=simple_model_spec_resolver.get_linkable_elements_for_metrics(
            metric_references=[MetricReference(element_name="bookings_per_view")],
            with_any_of=frozenset({LinkableElementProperties.METRIC_TIME}),
            without_any_of=frozenset(),
        ),
    )


def test_cyclic_join_manifest(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    cyclic_join_manifest_spec_resolver: ValidLinkableSpecResolver,
) -> None:
    assert_linkable_element_set_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        set_id="result0",
        linkable_element_set=cyclic_join_manifest_spec_resolver.get_linkable_elements_for_metrics(
            metric_references=[MetricReference(element_name="listings")],
            with_any_of=LinkableElementProperties.all_properties(),
            without_any_of=frozenset(),
        ),
    )


def test_create_linkable_element_set_from_join_path(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    simple_model_spec_resolver: ValidLinkableSpecResolver,
) -> None:
    assert_linkable_element_set_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        set_id="result0",
        linkable_element_set=simple_model_spec_resolver.create_linkable_element_set_from_join_path(
            join_path=SemanticModelJoinPath(
                path_elements=(
                    SemanticModelJoinPathElement(
                        semantic_model_reference=SemanticModelReference("listings_latest"),
                        join_on_entity=EntityReference("listing"),
                    ),
                )
            ),
            with_properties=frozenset({LinkableElementProperties.JOINED}),
        ),
    )


def test_create_linkable_element_set_from_join_path_multi_hop(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    simple_model_spec_resolver: ValidLinkableSpecResolver,
) -> None:
    assert_linkable_element_set_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        set_id="result0",
        linkable_element_set=simple_model_spec_resolver.create_linkable_element_set_from_join_path(
            join_path=SemanticModelJoinPath(
                path_elements=(
                    SemanticModelJoinPathElement(
                        semantic_model_reference=SemanticModelReference("listings_latest"),
                        join_on_entity=EntityReference("listing"),
                    ),
                )
            ),
            with_properties=frozenset({LinkableElementProperties.JOINED, LinkableElementProperties.MULTI_HOP}),
        ),
    )
