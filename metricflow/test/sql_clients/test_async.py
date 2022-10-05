import logging
import textwrap
import time
from typing import Optional

import pytest

from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.async_sql_client import AsyncSqlClient
from metricflow.protocols.sql_request import SqlRequestTagSet
from metricflow.sql_clients.sql_utils import make_df
from metricflow.test.compare_df import assert_dataframes_equal
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState

logger = logging.getLogger(__name__)


def create_table_with_n_rows(async_sql_client: AsyncSqlClient, schema_name: str, num_rows: int) -> SqlTable:
    """Create a table with a specific number of rows."""
    sql_table = SqlTable(
        schema_name=schema_name,
        table_name=f"table_with_{num_rows}_rows",
    )
    async_sql_client.drop_table(sql_table)
    async_sql_client.create_table_from_dataframe(
        sql_table=sql_table,
        df=make_df(sql_client=async_sql_client, columns=["example_string"], data=(("foo",) for _ in range(num_rows))),
    )
    return sql_table


def test_async_query(  # noqa: D
    async_sql_client: AsyncSqlClient, mf_test_session_state: MetricFlowTestSessionState
) -> None:
    request_id = async_sql_client.async_query("SELECT 1 AS foo")
    result = async_sql_client.async_request_result(request_id)
    assert_dataframes_equal(
        actual=result.df,
        expected=make_df(sql_client=async_sql_client, columns=["foo"], data=((1,),)),
    )
    assert result.exception is None


def test_async_execute(  # noqa: D
    async_sql_client: AsyncSqlClient, mf_test_session_state: MetricFlowTestSessionState
) -> None:
    request_id = async_sql_client.async_execute("SELECT 1 AS foo")
    result = async_sql_client.async_request_result(request_id)
    assert result.exception is None


def test_cancel_request(  # noqa: D
    async_sql_client: AsyncSqlClient, mf_test_session_state: MetricFlowTestSessionState
) -> None:
    if not async_sql_client.sql_engine_attributes.cancel_submitted_queries_supported:
        pytest.skip("Cancellation not yet supported in this SQL engine")
    # Execute a query that will be slow, giving the test the opportunity to cancel it.
    table_with_1000_rows = create_table_with_n_rows(
        async_sql_client, mf_test_session_state.mf_system_schema, num_rows=1000
    )
    table_with_100_rows = create_table_with_n_rows(
        async_sql_client, mf_test_session_state.mf_system_schema, num_rows=100
    )

    request_id = async_sql_client.async_execute(
        textwrap.dedent(
            f"""
            SELECT MAX({async_sql_client.sql_engine_attributes.random_function_name}()) AS max_value
            FROM {table_with_1000_rows.sql} a
            CROSS JOIN {table_with_1000_rows.sql} b
            CROSS JOIN {table_with_1000_rows.sql} c
            CROSS JOIN {table_with_100_rows.sql} d
            """
        )
    )
    # Need to wait a little bit as some clients like BQ doesn't show the query as running right away.
    start_time = time.time()
    num_cancelled: Optional[int] = None
    while time.time() - start_time < 30:
        time.sleep(1)
        num_cancelled = async_sql_client.cancel_request(SqlRequestTagSet.create_from_request_id(request_id))
        if num_cancelled > 0:
            break

    assert async_sql_client.async_request_result(request_id).exception is not None
    assert num_cancelled == 1
