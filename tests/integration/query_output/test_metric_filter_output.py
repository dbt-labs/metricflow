from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlClient
from tests.fixtures.setup_fixtures import MetricFlowTestConfiguration
from tests.integration.conftest import IntegrationTestHelpers
from tests.snapshot_utils import assert_str_snapshot_equal


@pytest.mark.sql_engine_snapshot
def test_query_with_simple_metric_in_where_filter(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["listings", "bookings"],
            group_by_names=["listing"],
            order_by_names=["listing"],
            where_constraint="{{ Metric('bookings', ['listing']) }} > 3",
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.to_string(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
def test_metric_with_metric_in_where_filter(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["active_listings", "bookings"],
            group_by_names=["listing"],
            order_by_names=["listing"],
            where_constraint="{{ Metric('bookings', ['listing']) }} > 1",
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.to_string(),
        sql_engine=sql_client.sql_engine_type,
    )
