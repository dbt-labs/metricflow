import logging
from typing import Sequence

import pytest

from metricflow.model.semantic_model import SemanticModel
from metricflow.model.semantics.linkable_spec_resolver import (
    ValidLinkableSpecResolver,
    LinkableElementProperties,
)
from metricflow.model.semantics.semantic_containers import MAX_JOIN_HOPS
from metricflow.specs import MetricSpec

logger = logging.getLogger(__name__)


@pytest.fixture
def simple_model_spec_resolver(simple_semantic_model: SemanticModel) -> ValidLinkableSpecResolver:  # noqa: D
    return ValidLinkableSpecResolver(
        user_configured_model=simple_semantic_model.user_configured_model,
        max_identifier_links=MAX_JOIN_HOPS,
    )


def test_linkable_spec_resolver(simple_model_spec_resolver: ValidLinkableSpecResolver) -> None:  # noqa: D
    result = simple_model_spec_resolver.get_linkable_elements_for_metrics(
        metric_specs=[MetricSpec(element_name="bookings"), MetricSpec(element_name="views")],
        with_any_of=LinkableElementProperties.all_properties(),
        without_any_of=frozenset({LinkableElementProperties.DERIVED_TIME_GRANULARITY}),
    ).as_spec_set

    assert [
        "create_a_cycle_in_the_join_graph__is_instant",
        "create_a_cycle_in_the_join_graph__listing__capacity_latest",
        "create_a_cycle_in_the_join_graph__listing__country_latest",
        "create_a_cycle_in_the_join_graph__listing__is_lux_latest",
        "listing__capacity_latest",
        "listing__country_latest",
        "listing__is_lux_latest",
        "listing__user__company_name",
        "listing__user__home_state",
        "listing__user__home_state_latest",
    ] == sorted(tuple(x.qualified_name for x in result.dimension_specs))
    assert [
        "create_a_cycle_in_the_join_graph__booking_paid_at",
        "create_a_cycle_in_the_join_graph__listing__created_at",
        "create_a_cycle_in_the_join_graph__listing__ds",
        "ds",
        "ds_partitioned",
        "listing__created_at",
        "listing__ds",
        "listing__user__created_at",
        "listing__user__ds_partitioned",
    ] == sorted(tuple(x.qualified_name for x in result.time_dimension_specs))
    assert [
        "create_a_cycle_in_the_join_graph",
        "create_a_cycle_in_the_join_graph__listing",
        "create_a_cycle_in_the_join_graph__listing__lux_listing",
        "create_a_cycle_in_the_join_graph__listing__user",
        "listing",
        "listing__lux_listing",
        "listing__user",
        "listing__user__company",
    ] == sorted(tuple(x.qualified_name for x in result.identifier_specs))


def property_check_helper(  # noqa: D
    spec_resolver: ValidLinkableSpecResolver,
    metric_specs: Sequence[MetricSpec],
    element_property: LinkableElementProperties,
    expected_names: Sequence[str],
) -> None:
    results = spec_resolver.get_linkable_elements_for_metrics(
        metric_specs=metric_specs,
        with_any_of=frozenset({element_property}),
        without_any_of=frozenset(),
    ).as_spec_set.as_tuple

    actual_names = sorted(tuple(x.qualified_name for x in results))
    assert expected_names == actual_names


def test_local_property(simple_model_spec_resolver: ValidLinkableSpecResolver) -> None:  # noqa: D
    property_check_helper(
        spec_resolver=simple_model_spec_resolver,
        metric_specs=[MetricSpec(element_name="listings")],
        element_property=LinkableElementProperties.LOCAL,
        expected_names=[
            "capacity_latest",
            "country_latest",
            "created_at",
            "created_at__month",
            "created_at__quarter",
            "created_at__week",
            "created_at__year",
            "ds",
            "ds__month",
            "ds__quarter",
            "ds__week",
            "ds__year",
            "is_lux_latest",
            "listing",
            "listing__capacity_latest",
            "listing__country_latest",
            "listing__created_at",
            "listing__created_at__month",
            "listing__created_at__quarter",
            "listing__created_at__week",
            "listing__created_at__year",
            "listing__ds",
            "listing__ds__month",
            "listing__ds__quarter",
            "listing__ds__week",
            "listing__ds__year",
            "listing__is_lux_latest",
            "user",
        ],
    )


def test_local_linked_property(simple_model_spec_resolver: ValidLinkableSpecResolver) -> None:  # noqa: D
    property_check_helper(
        spec_resolver=simple_model_spec_resolver,
        metric_specs=[MetricSpec(element_name="listings")],
        element_property=LinkableElementProperties.LOCAL_LINKED,
        expected_names=[
            "listing__capacity_latest",
            "listing__country_latest",
            "listing__created_at",
            "listing__created_at__month",
            "listing__created_at__quarter",
            "listing__created_at__week",
            "listing__created_at__year",
            "listing__ds",
            "listing__ds__month",
            "listing__ds__quarter",
            "listing__ds__week",
            "listing__ds__year",
            "listing__is_lux_latest",
        ],
    )


def test_joined_property(simple_model_spec_resolver: ValidLinkableSpecResolver) -> None:  # noqa: D
    property_check_helper(
        spec_resolver=simple_model_spec_resolver,
        metric_specs=[MetricSpec(element_name="listings")],
        element_property=LinkableElementProperties.JOINED,
        expected_names=[
            "listing__lux_listing",
            "user__company",
            "user__company_name",
            "user__created_at",
            "user__created_at__month",
            "user__created_at__quarter",
            "user__created_at__week",
            "user__created_at__year",
            "user__ds_partitioned",
            "user__ds_partitioned__month",
            "user__ds_partitioned__quarter",
            "user__ds_partitioned__week",
            "user__ds_partitioned__year",
            "user__home_state",
            "user__home_state_latest",
        ],
    )


def test_multi_hop_property(multi_hop_join_semantic_model: SemanticModel) -> None:  # noqa: D
    multi_hop_spec_resolver = ValidLinkableSpecResolver(
        user_configured_model=multi_hop_join_semantic_model.user_configured_model,
        max_identifier_links=MAX_JOIN_HOPS,
    )
    property_check_helper(
        spec_resolver=multi_hop_spec_resolver,
        metric_specs=[MetricSpec(element_name="txn_count")],
        element_property=LinkableElementProperties.MULTI_HOP,
        expected_names=[
            "account_id__customer_id__country",
            "account_id__customer_id__customer_atomic_weight",
            "account_id__customer_id__customer_name",
            "account_id__customer_id__customer_third_hop_id",
            "account_id__customer_id__ds_partitioned",
            "account_id__customer_id__ds_partitioned__month",
            "account_id__customer_id__ds_partitioned__quarter",
            "account_id__customer_id__ds_partitioned__week",
            "account_id__customer_id__ds_partitioned__year",
        ],
    )


def test_derived_time_granularity_property(simple_model_spec_resolver: ValidLinkableSpecResolver) -> None:  # noqa: D
    property_check_helper(
        spec_resolver=simple_model_spec_resolver,
        metric_specs=[MetricSpec(element_name="listings")],
        element_property=LinkableElementProperties.DERIVED_TIME_GRANULARITY,
        expected_names=[
            "created_at__month",
            "created_at__quarter",
            "created_at__week",
            "created_at__year",
            "ds__month",
            "ds__quarter",
            "ds__week",
            "ds__year",
            "listing__created_at__month",
            "listing__created_at__quarter",
            "listing__created_at__week",
            "listing__created_at__year",
            "listing__ds__month",
            "listing__ds__quarter",
            "listing__ds__week",
            "listing__ds__year",
            "user__created_at__month",
            "user__created_at__quarter",
            "user__created_at__week",
            "user__created_at__year",
            "user__ds_partitioned__month",
            "user__ds_partitioned__quarter",
            "user__ds_partitioned__week",
            "user__ds_partitioned__year",
        ],
    )


def test_identifier_property(simple_model_spec_resolver: ValidLinkableSpecResolver) -> None:  # noqa: D
    property_check_helper(
        spec_resolver=simple_model_spec_resolver,
        metric_specs=[MetricSpec(element_name="listings")],
        element_property=LinkableElementProperties.IDENTIFIER,
        expected_names=["listing", "listing__lux_listing", "user", "user__company"],
    )
