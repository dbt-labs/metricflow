import pytest

from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.semantics.data_source_container import PydanticDataSourceContainer
from metricflow.model.semantics.semantic_containers import DataSourceSemantics, MetricSemantics
from metricflow.specs import DimensionReference, MetricSpec, MeasureReference


@pytest.fixture
def new_data_source_semantics(simple_user_configured_model: UserConfiguredModel) -> DataSourceSemantics:  # Noqa: D
    return DataSourceSemantics(
        model=simple_user_configured_model,
        configured_data_source_container=PydanticDataSourceContainer(simple_user_configured_model.data_sources),
    )


@pytest.fixture
def new_metric_semantics(  # Noqa: D
    simple_user_configured_model: UserConfiguredModel, new_data_source_semantics: DataSourceSemantics
) -> MetricSemantics:
    return MetricSemantics(
        user_configured_model=simple_user_configured_model,
        data_source_semantics=new_data_source_semantics,
    )


def test_get_names(new_data_source_semantics: DataSourceSemantics) -> None:  # noqa: D
    expected = [
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
    assert sorted([d.element_name for d in new_data_source_semantics.get_dimension_references()]) == expected

    expected = [
        "average_booking_value",
        "bookers",
        "booking_value",
        "bookings",
        "identity_verifications",
        "instant_bookings",
        "largest_listing",
        "listings",
        "max_booking_value",
        "min_booking_value",
        "smallest_listing",
        "txn_revenue",
        "views",
    ]
    assert sorted([m.element_name for m in new_data_source_semantics.measure_references]) == expected

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
    assert sorted([i.element_name for i in new_data_source_semantics.get_identifier_references()]) == expected


def test_get_elements(new_data_source_semantics: DataSourceSemantics) -> None:  # noqa: D
    for dimension_reference in new_data_source_semantics.get_dimension_references():
        assert (
            new_data_source_semantics.get_dimension(dimension_reference=dimension_reference).name == dimension_reference
        )
    for measure_reference in new_data_source_semantics.measure_references:
        measure_reference = MeasureReference(element_name=measure_reference.element_name)
        assert new_data_source_semantics.get_measure(measure_reference=measure_reference).name == measure_reference


def test_get_data_sources_for_measure(new_data_source_semantics: DataSourceSemantics) -> None:  # noqa: D
    bookings_sources = new_data_source_semantics.get_data_sources_for_measure(MeasureReference(element_name="bookings"))
    assert len(bookings_sources) == 1
    assert bookings_sources[0].name == "bookings_source"

    views_sources = new_data_source_semantics.get_data_sources_for_measure(MeasureReference(element_name="views"))
    assert len(views_sources) == 1
    assert views_sources[0].name == "views_source"

    listings_sources = new_data_source_semantics.get_data_sources_for_measure(MeasureReference(element_name="listings"))
    assert len(listings_sources) == 1
    assert listings_sources[0].name == "listings_latest"


def test_dimension_is_partitioned(new_data_source_semantics: DataSourceSemantics) -> None:  # noqa: D
    assert new_data_source_semantics.dimension_is_partitioned(DimensionReference(element_name="ds_partitioned")) is True
    assert new_data_source_semantics.dimension_is_partitioned(DimensionReference(element_name="ds")) is False


def test_elements_for_measure(new_metric_semantics: MetricSemantics) -> None:  # noqa: D
    assert set(
        [x.qualified_name for x in new_metric_semantics.element_specs_for_metrics([MetricSpec(element_name="views")])]
    ) == {
        "create_a_cycle_in_the_join_graph",
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

    assert set(
        [
            x.qualified_name
            for x in new_metric_semantics.element_specs_for_metrics([MetricSpec(element_name="views")], local_only=True)
        ]
    ) == {
        "create_a_cycle_in_the_join_graph",
        "ds",
        "ds_partitioned",
        "listing",
        "user",
    }
