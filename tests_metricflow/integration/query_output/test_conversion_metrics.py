from __future__ import annotations

import datetime

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.integration.conftest import IntegrationTestHelpers
from tests_metricflow.snapshot_utils import assert_str_snapshot_equal


@pytest.mark.sql_engine_snapshot
def test_conversion_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test query against a conversion metric."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=("visit_buy_conversion_rate",),
            group_by_names=("metric_time",),
            order_by_names=("metric_time",),
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
def test_conversion_metric_with_window(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test query against a conversion metric with a window."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=("visit_buy_conversion_rate_7days",),
            group_by_names=("metric_time",),
            order_by_names=("metric_time",),
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
def test_conversion_metric_with_categorical_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test query against a conversion metric with a categorical filter."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=("visit_buy_conversion_rate",),
            group_by_names=("metric_time", "visit__referrer_id"),
            order_by_names=("metric_time", "visit__referrer_id"),
            where_constraints=("{{ Dimension('visit__referrer_id') }} = 'fb_ad_1'",),
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
def test_conversion_metric_with_time_constraint(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test query against a conversion metric with a time constraint and categorical filter."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=("visit_buy_conversion_rate",),
            group_by_names=("visit__referrer_id",),
            order_by_names=("visit__referrer_id",),
            where_constraints=("{{ Dimension('visit__referrer_id') }} = 'fb_ad_1'",),
            time_constraint_start=datetime.datetime(2020, 1, 1),
            time_constraint_end=datetime.datetime(2020, 1, 2),
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
def test_conversion_metric_with_window_and_time_constraint(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test query against a conversion metric with a window, time constraint, and categorical filter."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=("visit_buy_conversion_rate_7days",),
            group_by_names=(
                "metric_time",
                "visit__referrer_id",
            ),
            order_by_names=("metric_time", "visit__referrer_id"),
            where_constraints=("{{ Dimension('visit__referrer_id') }} = 'fb_ad_1'",),
            time_constraint_start=datetime.datetime(2020, 1, 1),
            time_constraint_end=datetime.datetime(2020, 1, 2),
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
def test_conversion_metric_with_filter_not_in_group_by(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test query against a conversion metric with a filter that doesn't exist in group by."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=("visit_buy_conversions",),
            time_constraint_start=datetime.datetime(2020, 1, 1),
            time_constraint_end=datetime.datetime(2020, 1, 1),
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
def test_conversion_metric_with_different_time_dimension_grains(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test query against a conversion metric with a filter that doesn't exist in group by."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=("visit_buy_conversion_rate_with_monthly_conversion",),
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
def test_conversion_metric_with_metric_definition_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test query against a conversion metric that has a filter defined in the YAML metric definition."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=("visit_buy_conversion_rate_with_filter",),
            group_by_names=("metric_time",),
            order_by_names=("metric_time",),
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
