from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlClient
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.integration.conftest import IntegrationTestHelpers
from metricflow.test.snapshot_utils import assert_str_snapshot_equal


@pytest.mark.sql_engine_snapshot
def test_offset_to_grain_with_single_granularity(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_at_start_of_month"],
            group_by_names=["metric_time__day"],
            order_by_names=["metric_time__day"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.to_string(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
def test_offset_to_grain_with_multiple_granularities(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_at_start_of_month"],
            group_by_names=["metric_time__day", "metric_time__month", "metric_time__year"],
            order_by_names=["metric_time__day", "metric_time__month", "metric_time__year"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.to_string(),
        sql_engine=sql_client.sql_engine_type,
    )
