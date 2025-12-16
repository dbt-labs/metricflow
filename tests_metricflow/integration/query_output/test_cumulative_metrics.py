from __future__ import annotations

import datetime

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.test_utils import as_datetime
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.integration.conftest import IntegrationTestHelpers
from tests_metricflow.snapshot_utils import assert_str_snapshot_equal


@pytest.mark.sql_engine_snapshot
def test_simple_cumulative_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Tests a query of a cumulative metric with a monthly window and a time constraint adjustment."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["trailing_2_months_revenue"],
            group_by_names=["metric_time"],
            order_by_names=["metric_time"],
            time_constraint_start=datetime.datetime(2020, 2, 1),
            time_constraint_end=datetime.datetime(2020, 4, 30),
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
def test_multiple_cumulative_metrics(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Tests a query with multiple cumulative metrics to ensure date selections align."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["revenue_all_time", "trailing_2_months_revenue"],
            group_by_names=["metric_time"],
            order_by_names=["metric_time"],
            time_constraint_start=datetime.datetime(2020, 3, 31),
            time_constraint_end=datetime.datetime(2020, 5, 31),
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
def test_non_additive_cumulative_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Tests a query with a non-additive cumulative metric to ensure the non-additive constraint is applied."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["every_two_days_bookers"],
            group_by_names=["metric_time"],
            order_by_names=["metric_time"],
            time_constraint_start=datetime.datetime(2019, 12, 31),
            time_constraint_end=datetime.datetime(2020, 1, 3),
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
def test_grain_to_date_cumulative_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Tests a month to date cumulative metric with a constraint to ensure all necessary input data is included."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["revenue_mtd"],
            group_by_names=["metric_time"],
            order_by_names=["metric_time"],
            time_constraint_start=datetime.datetime(2021, 1, 3),
            time_constraint_end=datetime.datetime(2021, 1, 6),
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
def test_cumulative_metric_with_non_adjustable_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Tests a cumulative metric with a filter that cannot be adjusted to ensure all data is included."""
    # Handle ds expression based on engine to support Trino.
    first_ds_expr = f"CAST('2020-03-15' AS {sql_client.sql_plan_renderer.expr_renderer.timestamp_data_type})"
    second_ds_expr = f"CAST('2020-04-30' AS {sql_client.sql_plan_renderer.expr_renderer.timestamp_data_type})"
    where_constraint = f"{{{{ TimeDimension('metric_time', 'day') }}}} = {first_ds_expr} or"
    where_constraint += f" {{{{ TimeDimension('metric_time', 'day') }}}} = {second_ds_expr}"

    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["trailing_2_months_revenue"],
            group_by_names=["metric_time"],
            order_by_names=["metric_time"],
            where_constraints=[where_constraint],
            time_constraint_end=as_datetime("2020-12-31"),
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


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_cumulative_metric_with_metric_definition_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Tests a cumulative metric that has a filter defined in the YAML metric definition."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["trailing_2_months_revenue_with_filter"],
            group_by_names=["metric_time"],
            order_by_names=["metric_time"],
            time_constraint_start=datetime.datetime(2020, 2, 1),
            time_constraint_end=datetime.datetime(2020, 4, 30),
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
