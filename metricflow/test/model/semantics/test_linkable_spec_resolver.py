from __future__ import annotations

import logging
from typing import Sequence

import pytest
from dbt_semantic_interfaces.references import MetricReference

from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.model.semantics.linkable_element_properties import LinkableElementProperties
from metricflow.model.semantics.linkable_spec_resolver import (
    ValidLinkableSpecResolver,
)
from metricflow.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS

logger = logging.getLogger(__name__)


@pytest.fixture
def simple_model_spec_resolver(  # noqa: D
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> ValidLinkableSpecResolver:
    return ValidLinkableSpecResolver(
        semantic_manifest=simple_semantic_manifest_lookup.semantic_manifest,
        semantic_model_lookup=simple_semantic_manifest_lookup.semantic_model_lookup,
        max_entity_links=MAX_JOIN_HOPS,
    )


@pytest.fixture
def cyclic_join_manifest_spec_resolver(  # noqa: D
    cyclic_join_semantic_manifest_lookup: SemanticManifestLookup,
) -> ValidLinkableSpecResolver:
    return ValidLinkableSpecResolver(
        semantic_manifest=cyclic_join_semantic_manifest_lookup.semantic_manifest,
        semantic_model_lookup=cyclic_join_semantic_manifest_lookup.semantic_model_lookup,
        max_entity_links=MAX_JOIN_HOPS,
    )


def test_linkable_spec_resolver(simple_model_spec_resolver: ValidLinkableSpecResolver) -> None:  # noqa: D
    result = simple_model_spec_resolver.get_linkable_elements_for_metrics(
        metric_references=[MetricReference(element_name="bookings"), MetricReference(element_name="views")],
        with_any_of=LinkableElementProperties.all_properties(),
        without_any_of=frozenset(
            {
                LinkableElementProperties.DERIVED_TIME_GRANULARITY,
            }
        ),
    ).as_spec_set

    assert [
        "listing__capacity_latest",
        "listing__country_latest",
        "listing__is_lux_latest",
        "listing__user__company_name",
        "listing__user__home_state",
        "listing__user__home_state_latest",
    ] == sorted(tuple(x.qualified_name for x in result.dimension_specs))
    assert [
        "ds",
        "ds_partitioned",
        "listing__created_at",
        "listing__ds",
        "listing__user__created_at",
        "listing__user__ds_partitioned",
        "metric_time",
    ] == sorted(tuple(x.qualified_name for x in result.time_dimension_specs))
    assert [
        "listing",
        "listing__lux_listing",
        "listing__user",
        "listing__user__company",
    ] == sorted(tuple(x.qualified_name for x in result.entity_specs))


def test_primary_entity(simple_model_spec_resolver: ValidLinkableSpecResolver) -> None:
    """Checks that local dimensions show up with the primary entity."""
    result = simple_model_spec_resolver.get_linkable_elements_for_metrics(
        metric_references=(MetricReference(element_name="bookings"),),
        with_any_of=LinkableElementProperties.all_properties(),
        without_any_of=frozenset(),
    ).as_spec_set

    assert sorted(tuple(spec.qualified_name for spec in result.as_tuple)) == [
        "booking__ds",
        "booking__ds__month",
        "booking__ds__quarter",
        "booking__ds__week",
        "booking__ds__year",
        "booking__ds_partitioned",
        "booking__ds_partitioned__month",
        "booking__ds_partitioned__quarter",
        "booking__ds_partitioned__week",
        "booking__ds_partitioned__year",
        "booking__guest",
        "booking__host",
        "booking__is_instant",
        "booking__listing",
        "booking__paid_at",
        "booking__paid_at__month",
        "booking__paid_at__quarter",
        "booking__paid_at__week",
        "booking__paid_at__year",
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
        "listing__lux_listing",
        "listing__user",
        "listing__user__company",
        "listing__user__company_name",
        "listing__user__created_at",
        "listing__user__created_at__month",
        "listing__user__created_at__quarter",
        "listing__user__created_at__week",
        "listing__user__created_at__year",
        "listing__user__ds_partitioned",
        "listing__user__ds_partitioned__month",
        "listing__user__ds_partitioned__quarter",
        "listing__user__ds_partitioned__week",
        "listing__user__ds_partitioned__year",
        "listing__user__home_state",
        "listing__user__home_state_latest",
        "metric_time",
        "metric_time__month",
        "metric_time__quarter",
        "metric_time__week",
        "metric_time__year",
    ]


def property_check_helper(  # noqa: D
    spec_resolver: ValidLinkableSpecResolver,
    metric_references: Sequence[MetricReference],
    element_property: LinkableElementProperties,
    expected_names: Sequence[str],
) -> None:
    results = spec_resolver.get_linkable_elements_for_metrics(
        metric_references=metric_references,
        with_any_of=frozenset({element_property}),
        without_any_of=frozenset(),
    ).as_spec_set.as_tuple

    actual_names = sorted(tuple(x.qualified_name for x in results))
    assert expected_names == actual_names


def test_local_property(simple_model_spec_resolver: ValidLinkableSpecResolver) -> None:  # noqa: D
    property_check_helper(
        spec_resolver=simple_model_spec_resolver,
        metric_references=[MetricReference(element_name="listings")],
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
        metric_references=[MetricReference(element_name="listings")],
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
        metric_references=[MetricReference(element_name="listings")],
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


def test_multi_hop_property(multi_hop_join_semantic_manifest_lookup: SemanticManifestLookup) -> None:  # noqa: D
    multi_hop_spec_resolver = ValidLinkableSpecResolver(
        semantic_manifest=multi_hop_join_semantic_manifest_lookup.semantic_manifest,
        semantic_model_lookup=multi_hop_join_semantic_manifest_lookup.semantic_model_lookup,
        max_entity_links=MAX_JOIN_HOPS,
    )
    property_check_helper(
        spec_resolver=multi_hop_spec_resolver,
        metric_references=[MetricReference(element_name="txn_count")],
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
        metric_references=[MetricReference(element_name="listings")],
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
            "metric_time__month",
            "metric_time__quarter",
            "metric_time__week",
            "metric_time__year",
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


def test_entity_property(simple_model_spec_resolver: ValidLinkableSpecResolver) -> None:  # noqa: D
    property_check_helper(
        spec_resolver=simple_model_spec_resolver,
        metric_references=[MetricReference(element_name="listings")],
        element_property=LinkableElementProperties.ENTITY,
        expected_names=["listing", "listing__lux_listing", "user", "user__company"],
    )


def test_metric_time_property_for_cumulative_metric(  # noqa: D
    simple_model_spec_resolver: ValidLinkableSpecResolver,
) -> None:
    property_check_helper(
        spec_resolver=simple_model_spec_resolver,
        metric_references=[MetricReference(element_name="trailing_2_months_revenue")],
        element_property=LinkableElementProperties.METRIC_TIME,
        expected_names=["metric_time"],
    )


def test_metric_time_property_for_derived_metrics(  # noqa: D
    simple_model_spec_resolver: ValidLinkableSpecResolver,
) -> None:
    property_check_helper(
        spec_resolver=simple_model_spec_resolver,
        metric_references=[MetricReference(element_name="bookings_per_view")],
        element_property=LinkableElementProperties.METRIC_TIME,
        expected_names=[
            "metric_time",
            "metric_time__month",
            "metric_time__quarter",
            "metric_time__week",
            "metric_time__year",
        ],
    )


def test_cyclic_join_manifest(  # noqa: D
    cyclic_join_manifest_spec_resolver: ValidLinkableSpecResolver,
) -> None:
    result = cyclic_join_manifest_spec_resolver.get_linkable_elements_for_metrics(
        metric_references=[MetricReference(element_name="listings")],
        with_any_of=LinkableElementProperties.all_properties(),
        without_any_of=frozenset(
            {
                LinkableElementProperties.DERIVED_TIME_GRANULARITY,
            }
        ),
    ).as_spec_set

    assert [
        "country_latest",
        "cyclic_entity",
        "cyclic_entity__capacity_latest",
        "cyclic_entity__listing",
        "ds",
        "listing",
        "listing__capacity_latest",
        "listing__country_latest",
        "listing__cyclic_entity",
        "listing__ds",
        "metric_time",
    ] == sorted([spec.qualified_name for spec in result.as_tuple])
