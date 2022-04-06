import logging

from metricflow.model.semantic_model import SemanticModel
from metricflow.model.semantics.linkable_spec_resolver import (
    ValidLinkableSpecResolver,
    LinkableElementProperties,
)
from metricflow.model.semantics.semantic_containers import MAX_JOIN_HOPS
from metricflow.specs import MetricSpec

logger = logging.getLogger(__name__)


def test_linkable_spec_resolver(simple_semantic_model: SemanticModel) -> None:  # noqa: D
    resolver = ValidLinkableSpecResolver(
        user_configured_model=simple_semantic_model.user_configured_model,
        primary_time_dimension_reference=simple_semantic_model.data_source_semantics.primary_time_dimension_reference,
        max_identifier_links=MAX_JOIN_HOPS,
    )

    result = resolver.get_linkable_elements_for_metrics(
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
