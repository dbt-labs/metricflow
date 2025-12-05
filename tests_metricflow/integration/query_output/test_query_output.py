from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.specs.query_param_implementations import (
    DimensionOrEntityParameter,
    MetricParameter,
    OrderByParameter,
    TimeDimensionParameter,
)
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.integration.conftest import IntegrationTestHelpers
from tests_metricflow.snapshot_utils import assert_str_snapshot_equal


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_aliases_with_metrics(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    metric_param = MetricParameter(name="bookings", alias="bookings_alias")
    time_dimension_param = TimeDimensionParameter(name="metric_time__day", alias="booking_day")
    dimension_param = DimensionOrEntityParameter(name="listing__capacity_latest", alias="listing_capacity")
    entity_param = DimensionOrEntityParameter(name="listing", alias="listing_id")
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metrics=(metric_param,),
            group_by=(time_dimension_param, dimension_param, entity_param),
            order_by=(
                OrderByParameter(time_dimension_param),
                OrderByParameter(dimension_param),
                OrderByParameter(entity_param),
                OrderByParameter(metric_param),
            ),
            where_constraints=("{{ Metric('bookings', ['listing']) }} > 2",),
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_aliases_without_metrics(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    time_dimension_param = TimeDimensionParameter(name="metric_time__day", alias="booking_day")
    dimension_param = DimensionOrEntityParameter(name="listing__capacity_latest", alias="listing_capacity")
    entity_param = DimensionOrEntityParameter(name="listing", alias="listing_id")
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            group_by=(time_dimension_param, dimension_param, entity_param),
            order_by=(
                OrderByParameter(time_dimension_param),
                OrderByParameter(dimension_param),
                OrderByParameter(entity_param),
            ),
            where_constraints=("{{ Dimension('listing__capacity_latest') }} > 2",),
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_multiple_metrics_with_alias(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metrics=(
                MetricParameter(name="bookings", alias="bookings_alias"),
                MetricParameter(name="booking_fees", alias="bookings_fees_alias"),
            ),
            group_by_names=["metric_time__day"],
            order_by_names=["metric_time__day"],
            where_constraints=("{{ Metric('bookings', ['listing']) }} > 2",),
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_derived_metric_alias(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metrics=(MetricParameter(name="booking_fees", alias="booking_fees_alias"),),
            group_by_names=["metric_time__day"],
            order_by_names=["metric_time__day"],
            where_constraints=("{{ Metric('bookings', ['listing']) }} > 2",),
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_scd_with_coarser_grain(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    scd_it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = scd_it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metrics=(MetricParameter(name="family_bookings"),),
            group_by_names=["listing__capacity", "metric_time__month"],
            order_by_names=["listing__capacity", "metric_time__month"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_scd_group_by_without_metric_time(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    scd_it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = scd_it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metrics=(MetricParameter(name="family_bookings"),),
            group_by_names=["listing__capacity"],
            order_by_names=["listing__capacity"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_scd_filter_without_metric_time(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    scd_it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = scd_it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metrics=(MetricParameter(name="family_bookings"),),
            where_constraints=("{{ Dimension('listing__capacity') }} > 2",),
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_multiple_time_spines(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["subdaily_join_to_time_spine_metric", "subdaily_cumulative_window_metric"],
            group_by_names=["metric_time__alien_day", "metric_time__hour"],
            order_by_names=["metric_time__alien_day", "metric_time__hour"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_saved_query_with_order_by_and_limit(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(saved_query_name="p0_booking_with_order_by_and_limit")
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_saved_query_override_order_by_and_limit(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            saved_query_name="p0_booking_with_order_by_and_limit",
            order_by_names=["bookings", "views", "listing__capacity_latest", "metric_time__day"],
            limit=5,
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_semi_additive_measure_with_where_filter(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["current_account_balance_by_user"],
            group_by_names=["user"],
            order_by_names=["user"],
            where_constraints=("{{ Dimension('account__account_type') }} = 'savings'",),
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )
