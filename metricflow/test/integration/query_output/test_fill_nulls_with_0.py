from __future__ import annotations

import datetime

import pytest
from _pytest.fixtures import FixtureRequest

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlClient
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.integration.conftest import IntegrationTestHelpers
from metricflow.test.snapshot_utils import assert_object_snapshot_equal


@pytest.mark.sql_engine_snapshot
def test_simple_fill_nulls_with_0_metric_time(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_fill_nulls_with_0"],
            group_by_names=["metric_time"],
            order_by_names=["metric_time"],
            time_constraint_start=datetime.datetime(2019, 11, 27),
            time_constraint_end=datetime.datetime(2020, 1, 5),
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_object_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        obj_id="query_output",
        obj=query_result.result_df.to_string(),
        sql_client=sql_client,
    )


@pytest.mark.sql_engine_snapshot
def test_simple_fill_nulls_with_0_month(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_fill_nulls_with_0"],
            group_by_names=["metric_time__month"],
            order_by_names=["metric_time__month"],
            time_constraint_start=datetime.datetime(2019, 1, 1),
            time_constraint_end=datetime.datetime(2020, 12, 1),
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_object_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        obj_id="query_output",
        obj=query_result.result_df.to_string(),
        sql_client=sql_client,
    )


@pytest.mark.sql_engine_snapshot
def test_simple_join_to_time_spine(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_join_to_time_spine"],
            group_by_names=["metric_time"],
            time_constraint_start=datetime.datetime(2019, 11, 27),
            time_constraint_end=datetime.datetime(2020, 1, 5),
            order_by_names=["metric_time"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_object_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        obj_id="query_output",
        obj=query_result.result_df.to_string(),
        sql_client=sql_client,
    )


@pytest.mark.sql_engine_snapshot
def test_fill_nulls_with_0_multi_metric_query(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_fill_nulls_with_0", "views"],
            group_by_names=["metric_time"],
            order_by_names=["metric_time"],
            time_constraint_start=datetime.datetime(2019, 11, 27),
            time_constraint_end=datetime.datetime(2020, 1, 5),
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_object_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        obj_id="query_output",
        obj=query_result.result_df.to_string(),
        sql_client=sql_client,
    )


@pytest.mark.sql_engine_snapshot
def test_fill_nulls_with_0_multi_metric_query_with_categorical_dimension(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_fill_nulls_with_0_without_time_spine", "views"],
            group_by_names=["metric_time", "listing__is_lux_latest"],
            order_by_names=["metric_time", "listing__is_lux_latest"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_object_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        obj_id="query_output",
        obj=query_result.result_df.to_string(),
        sql_client=sql_client,
    )
