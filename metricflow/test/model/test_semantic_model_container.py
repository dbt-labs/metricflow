from __future__ import annotations

import logging

import pytest
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.references import EntityReference, MeasureReference, MetricReference

from metricflow.model.semantics.linkable_element_properties import LinkableElementProperties
from metricflow.model.semantics.metric_lookup import MetricLookup
from metricflow.model.semantics.semantic_model_lookup import SemanticModelLookup

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


def test_get_names(semantic_model_lookup: SemanticModelLookup) -> None:  # noqa: D
    expected = [
        "account_type",
        "booking_paid_at",
        "capacity_latest",
        "company_name",
        "country_latest",
        "created_at",
        "ds",
        "ds_partitioned",
        "home_state",
        "home_state_latest",
        "is_instant",
        "is_lux_latest",
        "verification_type",
    ]
    assert sorted([d.element_name for d in semantic_model_lookup.get_dimension_references()]) == expected

    expected = [
        "account_balance",
        "approximate_continuous_booking_value_p99",
        "approximate_discrete_booking_value_p99",
        "average_booking_value",
        "bookers",
        "booking_payments",
        "booking_value",
        "booking_value_p99",
        "bookings",
        "current_account_balance_by_user",
        "discrete_booking_value_p99",
        "identity_verifications",
        "instant_bookings",
        "largest_listing",
        "listings",
        "max_booking_value",
        "median_booking_value",
        "min_booking_value",
        "referred_bookings",
        "smallest_listing",
        "total_account_balance_first_day",
        "txn_revenue",
        "views",
    ]
    assert sorted([m.element_name for m in semantic_model_lookup.measure_references]) == expected

    expected = [
        "company",
        "create_a_cycle_in_the_join_graph",
        "guest",
        "host",
        "listing",
        "lux_listing",
        "user",
        "verification",
    ]
    assert sorted([i.element_name for i in semantic_model_lookup.get_entity_references()]) == expected


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


def test_elements_for_metric(metric_lookup: MetricLookup) -> None:  # noqa: D
    assert set(
        [
            x.qualified_name
            for x in metric_lookup.element_specs_for_metrics(
                [MetricReference(element_name="views")],
                without_any_property=frozenset({LinkableElementProperties.DERIVED_TIME_GRANULARITY}),
            )
        ]
    ) == {
        "create_a_cycle_in_the_join_graph",
        "create_a_cycle_in_the_join_graph__booking_paid_at",
        "create_a_cycle_in_the_join_graph__guest",
        "create_a_cycle_in_the_join_graph__host",
        "create_a_cycle_in_the_join_graph__is_instant",
        "create_a_cycle_in_the_join_graph__listing",
        "create_a_cycle_in_the_join_graph__listing__ds",
        "create_a_cycle_in_the_join_graph__listing__capacity_latest",
        "create_a_cycle_in_the_join_graph__listing__country_latest",
        "create_a_cycle_in_the_join_graph__listing__created_at",
        "create_a_cycle_in_the_join_graph__listing__is_lux_latest",
        "create_a_cycle_in_the_join_graph__listing__user",
        "create_a_cycle_in_the_join_graph__listing__lux_listing",
        "ds",
        "ds_partitioned",
        "listing",
        "listing__capacity_latest",
        "listing__country_latest",
        "listing__created_at",
        "listing__ds",
        "listing__is_lux_latest",
        "listing__lux_listing",
        "listing__user",
        "listing__user__company",
        "listing__user__company_name",
        "listing__user__created_at",
        "listing__user__ds_partitioned",
        "listing__user__home_state",
        "listing__user__home_state_latest",
        "user",
        "user__company",
        "user__company_name",
        "user__created_at",
        "user__ds_partitioned",
        "user__home_state",
        "user__home_state_latest",
    }

    local_specs = metric_lookup.element_specs_for_metrics(
        metric_references=[MetricReference(element_name="views")],
        with_any_property=frozenset({LinkableElementProperties.LOCAL}),
        without_any_property=frozenset({LinkableElementProperties.DERIVED_TIME_GRANULARITY}),
    )
    assert set([x.qualified_name for x in local_specs]) == {
        "create_a_cycle_in_the_join_graph",
        "ds",
        "ds_partitioned",
        "listing",
        "user",
    }


def test_local_linked_elements_for_metric(metric_lookup: MetricLookup) -> None:  # noqa: D
    result = set(
        [
            x.qualified_name
            for x in metric_lookup.element_specs_for_metrics(
                [MetricReference(element_name="listings")],
                with_any_property=frozenset({LinkableElementProperties.LOCAL_LINKED}),
                without_any_property=frozenset({LinkableElementProperties.DERIVED_TIME_GRANULARITY}),
            )
        ]
    )

    assert result == {
        "listing__created_at",
        "listing__country_latest",
        "listing__is_lux_latest",
        "listing__ds",
        "listing__capacity_latest",
    }


def test_get_semantic_models_for_entity(semantic_model_lookup: SemanticModelLookup) -> None:  # noqa: D
    entity_reference = EntityReference(element_name="user")
    linked_semantic_models = semantic_model_lookup.get_semantic_models_for_entity(entity_reference=entity_reference)
    assert len(linked_semantic_models) == 8


def test_linkable_set(metric_lookup: MetricLookup) -> None:  # noqa: D
    linkable_set = metric_lookup.linkable_set_for_metrics(
        (MetricReference(element_name="views"),),
        without_any_property=frozenset({LinkableElementProperties.DERIVED_TIME_GRANULARITY}),
    )

    result_to_compare = sorted(
        tuple(
            (
                # Checking a limited set of fields as the result is large due to the paths in the object.
                linkable_dimension.entity_links,
                linkable_dimension.element_name,
                linkable_dimension.semantic_model_origin.semantic_model_name,
            )
            for linkable_dimensions in linkable_set.path_key_to_linkable_dimensions.values()
            for linkable_dimension in linkable_dimensions
        )
    )

    assert result_to_compare == [
        ((), "ds", "views_source"),
        ((), "ds_partitioned", "views_source"),
        (("create_a_cycle_in_the_join_graph",), "booking_paid_at", "bookings_source"),
        (("create_a_cycle_in_the_join_graph",), "is_instant", "bookings_source"),
        (("create_a_cycle_in_the_join_graph", "listing"), "capacity_latest", "listings_latest"),
        (("create_a_cycle_in_the_join_graph", "listing"), "country_latest", "listings_latest"),
        (("create_a_cycle_in_the_join_graph", "listing"), "created_at", "listings_latest"),
        (("create_a_cycle_in_the_join_graph", "listing"), "ds", "listings_latest"),
        (("create_a_cycle_in_the_join_graph", "listing"), "is_lux_latest", "listings_latest"),
        (("listing",), "capacity_latest", "listings_latest"),
        (("listing",), "country_latest", "listings_latest"),
        (("listing",), "created_at", "listings_latest"),
        (("listing",), "ds", "listings_latest"),
        (("listing",), "is_lux_latest", "listings_latest"),
        (("listing", "user"), "company_name", "companies"),
        (("listing", "user"), "created_at", "users_ds_source"),
        (("listing", "user"), "ds_partitioned", "users_ds_source"),
        (("listing", "user"), "home_state", "users_ds_source"),
        (("listing", "user"), "home_state_latest", "users_latest"),
        (("user",), "company_name", "companies"),
        (("user",), "created_at", "users_ds_source"),
        (("user",), "ds_partitioned", "users_ds_source"),
        (("user",), "home_state", "users_ds_source"),
        (("user",), "home_state_latest", "users_latest"),
    ]


def test_linkable_set_for_common_dimensions_in_different_models(metric_lookup: MetricLookup) -> None:  # noqa: D
    """Tests case where a metric has dimensions with the same path.

    In this example, "ds" is defined in both "bookings_source" and "views_source".
    """
    linkable_set = metric_lookup.linkable_set_for_metrics(
        (MetricReference(element_name="bookings_per_view"),),
        without_any_property=frozenset({LinkableElementProperties.DERIVED_TIME_GRANULARITY}),
    )

    result_to_compare = sorted(
        tuple(
            (
                # Checking a limited set of fields as the result is large due to the paths in the object.
                linkable_dimension.entity_links,
                linkable_dimension.element_name,
                linkable_dimension.semantic_model_origin.semantic_model_name,
                # Abbreviated version of the join path.
                tuple(
                    join_path_element.semantic_model_reference.semantic_model_name
                    for join_path_element in linkable_dimension.join_path
                ),
            )
            for linkable_dimensions in linkable_set.path_key_to_linkable_dimensions.values()
            for linkable_dimension in linkable_dimensions
        )
    )

    assert result_to_compare == [
        ((), "ds", "bookings_source", ()),
        ((), "ds", "views_source", ()),
        ((), "ds_partitioned", "bookings_source", ()),
        ((), "ds_partitioned", "views_source", ()),
        (("create_a_cycle_in_the_join_graph",), "booking_paid_at", "bookings_source", ()),
        (("create_a_cycle_in_the_join_graph",), "booking_paid_at", "bookings_source", ()),
        (("create_a_cycle_in_the_join_graph",), "is_instant", "bookings_source", ()),
        (("create_a_cycle_in_the_join_graph",), "is_instant", "bookings_source", ("bookings_source",)),
        (
            ("create_a_cycle_in_the_join_graph", "listing"),
            "capacity_latest",
            "listings_latest",
            ("bookings_source", "listings_latest"),
        ),
        (
            ("create_a_cycle_in_the_join_graph", "listing"),
            "capacity_latest",
            "listings_latest",
            ("views_source", "listings_latest"),
        ),
        (
            ("create_a_cycle_in_the_join_graph", "listing"),
            "country_latest",
            "listings_latest",
            ("bookings_source", "listings_latest"),
        ),
        (
            ("create_a_cycle_in_the_join_graph", "listing"),
            "country_latest",
            "listings_latest",
            ("views_source", "listings_latest"),
        ),
        (("create_a_cycle_in_the_join_graph", "listing"), "created_at", "listings_latest", ()),
        (("create_a_cycle_in_the_join_graph", "listing"), "ds", "listings_latest", ()),
        (
            ("create_a_cycle_in_the_join_graph", "listing"),
            "is_lux_latest",
            "listings_latest",
            ("bookings_source", "listings_latest"),
        ),
        (
            ("create_a_cycle_in_the_join_graph", "listing"),
            "is_lux_latest",
            "listings_latest",
            ("views_source", "listings_latest"),
        ),
        (("listing",), "capacity_latest", "listings_latest", ("listings_latest",)),
        (("listing",), "country_latest", "listings_latest", ("listings_latest",)),
        (("listing",), "created_at", "listings_latest", ()),
        (("listing",), "ds", "listings_latest", ()),
        (("listing",), "is_lux_latest", "listings_latest", ("listings_latest",)),
        (("listing", "user"), "company_name", "companies", ("listings_latest", "companies")),
        (("listing", "user"), "created_at", "users_ds_source", ()),
        (("listing", "user"), "ds_partitioned", "users_ds_source", ()),
        (("listing", "user"), "home_state", "users_ds_source", ("listings_latest", "users_ds_source")),
        (("listing", "user"), "home_state_latest", "users_latest", ("listings_latest", "users_latest")),
    ]
