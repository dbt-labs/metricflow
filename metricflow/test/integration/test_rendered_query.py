from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest

from metricflow.dataflow.sql_table import SqlTable
from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.integration.conftest import IntegrationTestHelpers
from metricflow.test.snapshot_utils import (
    assert_sql_snapshot_equal,
)


@pytest.mark.sql_engine_snapshot
def test_render_query(  # noqa: D
    request: FixtureRequest, mf_test_session_state: MetricFlowTestSessionState, it_helpers: IntegrationTestHelpers
) -> None:
    result = it_helpers.mf_engine.explain(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings"],
            group_by_names=["metric_time"],
        )
    )

    assert_sql_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        snapshot_id="query0",
        sql=result.rendered_sql.sql_query,
        sql_engine=it_helpers.sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
def test_render_write_to_table_query(  # noqa: D
    request: FixtureRequest, mf_test_session_state: MetricFlowTestSessionState, it_helpers: IntegrationTestHelpers
) -> None:
    output_table = SqlTable(schema_name=it_helpers.mf_system_schema, table_name="test_table")

    result = it_helpers.mf_engine.explain(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings"], group_by_names=["metric_time"], output_table=output_table.sql
        )
    )

    assert_sql_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        snapshot_id="query0",
        sql=result.rendered_sql.sql_query,
        sql_engine=it_helpers.sql_client.sql_engine_type,
    )
