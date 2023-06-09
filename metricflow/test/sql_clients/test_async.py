from __future__ import annotations

import json
import logging

import pytest
from dbt_semantic_interfaces.enum_extension import assert_values_exhausted

from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SqlClient, SqlEngine
from metricflow.protocols.sql_request import MF_EXTRA_TAGS_KEY, SqlJsonTag
from metricflow.sql_clients.sql_utils import make_df
from metricflow.test.compare_df import assert_dataframes_equal
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState

logger = logging.getLogger(__name__)


def create_table_with_n_rows(sql_client: SqlClient, schema_name: str, num_rows: int) -> SqlTable:
    """Create a table with a specific number of rows."""
    sql_table = SqlTable(
        schema_name=schema_name,
        table_name=f"table_with_{num_rows}_rows",
    )
    sql_client.drop_table(sql_table)
    sql_client.create_table_from_dataframe(
        sql_table=sql_table,
        df=make_df(sql_client=sql_client, columns=["example_string"], data=(("foo",) for _ in range(num_rows))),
    )
    return sql_table


def test_async_query(sql_client: SqlClient, mf_test_session_state: MetricFlowTestSessionState) -> None:  # noqa: D
    request_id = sql_client.async_query("SELECT 1 AS foo")
    result = sql_client.async_request_result(request_id)
    assert_dataframes_equal(
        actual=result.df,
        expected=make_df(sql_client=sql_client, columns=["foo"], data=((1,),)),
    )
    assert result.exception is None


def test_async_execute(sql_client: SqlClient, mf_test_session_state: MetricFlowTestSessionState) -> None:  # noqa: D
    request_id = sql_client.async_execute("SELECT 1 AS foo")
    result = sql_client.async_request_result(request_id)
    assert result.exception is None


def test_isolation_level(mf_test_session_state: MetricFlowTestSessionState, sql_client: SqlClient) -> None:  # noqa: D
    for isolation_level in sql_client.sql_engine_attributes.supported_isolation_levels:
        logger.info(f"Testing isolation level: {isolation_level}")
        request_id = sql_client.async_query("SELECT 1", isolation_level=isolation_level)
        sql_client.async_request_result(request_id)


def test_request_tags(
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
) -> None:
    """Test whether request tags are appropriately used in queries to the SQL engine."""
    engine_type = sql_client.sql_engine_attributes.sql_engine_type
    extra_tags = SqlJsonTag({"example_key": "example_value"})
    if engine_type is SqlEngine.SNOWFLAKE:
        request_id0 = sql_client.async_query(
            "SHOW PARAMETERS LIKE 'QUERY_TAG'",
            extra_tags=extra_tags,
        )
        result0 = sql_client.async_request_result(request_id0)
        df = result0.df
        assert df is not None
        assert result0.exception is None

        assert len(df.index) == 1
        tag_json = json.loads(df.iloc[0]["value"])
        assert MF_EXTRA_TAGS_KEY in tag_json
        assert tag_json[MF_EXTRA_TAGS_KEY] == {"example_key": "example_value"}
    elif (
        engine_type is SqlEngine.DUCKDB
        or engine_type is SqlEngine.BIGQUERY
        or engine_type is SqlEngine.REDSHIFT
        or engine_type is SqlEngine.DATABRICKS
        or engine_type is SqlEngine.POSTGRES
    ):
        pytest.skip(f"Testing tags not supported in {engine_type}")
    else:
        assert_values_exhausted(engine_type)
