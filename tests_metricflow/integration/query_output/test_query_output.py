from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.specs.query_param_implementations import MetricParameter
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.integration.conftest import IntegrationTestHelpers
from tests_metricflow.snapshot_utils import assert_str_snapshot_equal


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_metric_alias(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metrics=(MetricParameter(name="bookings", alias="bookings_alias"),),
            group_by_names=["metric_time__day"],
            order_by_names=["metric_time__day", "bookings"],
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
