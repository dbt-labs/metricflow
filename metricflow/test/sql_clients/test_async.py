import json
import logging
import textwrap
import time

import pytest

from metricflow.dataflow.sql_table import SqlTable
from metricflow.object_utils import assert_values_exhausted
from metricflow.protocols.async_sql_client import AsyncSqlClient
from metricflow.protocols.sql_client import SqlEngine
from metricflow.protocols.sql_request import MF_EXTRA_TAGS_KEY, SqlJsonTag
from metricflow.sql_clients.async_request import CombinedSqlTags
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

    num_attempts = 3
    cancel_using_request_id_successful = False
    for attempt_num in range(num_attempts):
        if try_cancel_request(
            async_sql_client=async_sql_client,
            table_with_1000_rows=table_with_1000_rows,
            table_with_100_rows=table_with_100_rows,
            attempt_num=attempt_num,
            cancel_using_extra_tag=False,
        ):
            cancel_using_request_id_successful = True
            break

    assert (
        cancel_using_request_id_successful
    ), f"Was not able to cancel a request using the request ID after {num_attempts} attempts"

    cancel_using_extra_tag_successful = False
    for attempt_num in range(num_attempts - 1):
        if try_cancel_request(
            async_sql_client=async_sql_client,
            table_with_1000_rows=table_with_1000_rows,
            table_with_100_rows=table_with_100_rows,
            attempt_num=attempt_num,
            cancel_using_extra_tag=True,
        ):
            cancel_using_extra_tag_successful = True
            break

    assert (
        cancel_using_extra_tag_successful
    ), f"Was not able to cancel a request using the extra tag after {num_attempts} attempts"


def try_cancel_request(
    async_sql_client: AsyncSqlClient,
    table_with_1000_rows: SqlTable,
    table_with_100_rows: SqlTable,
    attempt_num: int,
    cancel_using_extra_tag: bool,
) -> bool:
    """Try to cancel a query and return True if successful."""
    extra_tag_key = "example_key"
    extra_tag_value = "example_value"

    request_id = async_sql_client.async_execute(
        textwrap.dedent(
            f"""
            SELECT MAX({async_sql_client.sql_engine_attributes.random_function_name}()) AS max_value
            FROM {table_with_1000_rows.sql} a
            CROSS JOIN {table_with_1000_rows.sql} b
            CROSS JOIN {table_with_1000_rows.sql} c
            CROSS JOIN {table_with_100_rows.sql} d
            """
        ),
        extra_tags=SqlJsonTag({extra_tag_key: extra_tag_value}),
    )
    # Need to wait a little bit as some clients like BQ doesn't show the query as running right away.
    start_time = time.time()
    num_cancelled = 0
    while time.time() - start_time < 30:
        time.sleep(1)

        if cancel_using_extra_tag:

            def _match_function(combined_tags: CombinedSqlTags) -> bool:
                return combined_tags.extra_tag.json_dict.get(extra_tag_key) == extra_tag_value

            num_cancelled = async_sql_client.cancel_request(_match_function)
        else:

            def _match_function(combined_tags: CombinedSqlTags) -> bool:
                return combined_tags.system_tags.request_id == request_id

            num_cancelled = async_sql_client.cancel_request(_match_function)

        if num_cancelled > 0:
            break

    result_exception = async_sql_client.async_request_result(request_id).exception
    if result_exception is not None and num_cancelled >= 1:
        assert num_cancelled == 1, f"Should have cancelled exactly 1 query, but instead cancelled {num_cancelled}"
        return True

    logger.warning(
        f"Cancellation did not occur successfully on attempt #{attempt_num}. This may be okay as SQL engines do not "
        f"guarantee cancellation requests: num_cancelled={num_cancelled} result_exception={result_exception}"
    )
    return False


def test_isolation_level(  # noqa: D
    mf_test_session_state: MetricFlowTestSessionState, async_sql_client: AsyncSqlClient
) -> None:
    for isolation_level in async_sql_client.sql_engine_attributes.supported_isolation_levels:
        logger.info(f"Testing isolation level: {isolation_level}")
        request_id = async_sql_client.async_query("SELECT 1", isolation_level=isolation_level)
        async_sql_client.async_request_result(request_id)


def test_request_tags(
    mf_test_session_state: MetricFlowTestSessionState,
    async_sql_client: AsyncSqlClient,
) -> None:
    """Test whether request tags are appropriately used in queries to the SQL engine."""
    engine_type = async_sql_client.sql_engine_attributes.sql_engine_type
    extra_tags = SqlJsonTag({"example_key": "example_value"})
    if engine_type is SqlEngine.SNOWFLAKE:
        request_id0 = async_sql_client.async_query(
            "SHOW PARAMETERS LIKE 'QUERY_TAG'",
            extra_tags=extra_tags,
        )
        result0 = async_sql_client.async_request_result(request_id0)
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
        or engine_type is SqlEngine.MYSQL
    ):
        pytest.skip(f"Testing tags not supported in {engine_type}")
    else:
        assert_values_exhausted(engine_type)
