from __future__ import annotations

import datetime
import logging
import threading
from typing import Sequence, Set, Union

import pandas as pd
import pytest
from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from sqlalchemy.exc import ProgrammingError

from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SqlClient, SqlEngine
from metricflow.random_id import random_id
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql.sql_column_type import SqlColumnType
from metricflow.sql_clients.sql_utils import make_df
from metricflow.test.compare_df import assert_dataframes_equal
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState

logger = logging.getLogger(__name__)


def _random_table() -> str:
    return f"test_table_{random_id()}"


def _select_x_as_y(x: int = 1, y: str = "y") -> str:  # noqa: D
    return f"SELECT {x} AS {y}"


def _check_1col(df: pd.DataFrame, col: str = "y", vals: Set[Union[int, str]] = {1}) -> None:  # noqa: D
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (len(vals), 1)
    assert df.columns.tolist() == [col]
    assert set(df[col]) == vals


def test_query(sql_client: SqlClient) -> None:  # noqa: D
    df = sql_client.query(_select_x_as_y())
    _check_1col(df)


def test_query_with_execution_params(sql_client: SqlClient) -> None:
    """Test querying with execution parameters of all supported datatypes."""
    params: Sequence[SqlColumnType] = [
        2,
        "hi",
        3.5,
        True,
        False,
        datetime.datetime(2022, 1, 1),
        datetime.date(2020, 12, 31),
    ]
    for param in params:
        sql_execution_params = SqlBindParameters.create_from_dict(({"x": param}))
        assert sql_execution_params.param_dict["x"] == param  # check that pydantic did not coerce type unexpectedly

        expr = f"SELECT {sql_client.render_bind_parameter_key('x')} as y"
        df = sql_client.query(expr, sql_bind_parameters=sql_execution_params)
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (1, 1)
        assert df.columns.tolist() == ["y"]

        # Some engines convert some types to str; convert everything to str for comparison
        str_param = str(param)
        str_result = str(df["y"][0])
        # Some engines use JSON bool syntax (i.e., True -> 'true')
        if isinstance(param, bool):
            assert str_result in [str_param, str_param.lower()]
        # Some engines add decimals to datetime milliseconds; trim here
        elif isinstance(param, datetime.datetime):
            assert str_result[: len(str_param)] == str_param
        else:
            assert str_result == str_param


def test_select_one_query(sql_client: SqlClient) -> None:  # noqa: D
    sql_client.query("SELECT 1")
    with pytest.raises(Exception):
        sql_client.query("this is garbage")


def test_failed_query_with_execution_params(sql_client: SqlClient) -> None:  # noqa: D
    expr = f"SELECT {sql_client.render_bind_parameter_key('x')}"
    sql_execution_params = SqlBindParameters.create_from_dict({"x": 1})

    sql_client.query(expr, sql_bind_parameters=sql_execution_params)
    with pytest.raises(Exception):
        sql_client.query("this is garbage")


def test_create_table(mf_test_session_state: MetricFlowTestSessionState, sql_client: SqlClient) -> None:  # noqa: D
    sql_table = SqlTable(schema_name=mf_test_session_state.mf_system_schema, table_name=_random_table())
    sql_client.create_table_as_select(sql_table, _select_x_as_y())
    df = sql_client.query(f"SELECT * FROM {sql_table.sql}")
    _check_1col(df)


def test_create_table_from_dataframe(  # noqa: D
    mf_test_session_state: MetricFlowTestSessionState, sql_client: SqlClient
) -> None:
    expected_df = make_df(
        sql_client=sql_client,
        columns=["int_col", "str_col", "float_col", "bool_col", "time_col"],
        time_columns={"time_col"},
        data=[
            (1, "abc", 1.23, False, "2020-01-01"),
            (2, "def", 4.56, True, "2020-01-02"),
            (3, "ghi", 1.1, False, None),  # Test NaT type
            (None, "jkl", None, True, "2020-01-03"),  # Test NaN types
            (3, None, 1.2, None, "2020-01-04"),  # Test None types for NA conversions
        ],
    )
    sql_table = SqlTable(schema_name=mf_test_session_state.mf_system_schema, table_name=_random_table())
    sql_client.create_table_from_dataframe(sql_table=sql_table, df=expected_df)

    actual_df = sql_client.query(f"SELECT * FROM {sql_table.sql}")
    assert_dataframes_equal(actual=actual_df, expected=expected_df)


def test_table_exists(mf_test_session_state: MetricFlowTestSessionState, sql_client: SqlClient) -> None:  # noqa: D
    sql_table = SqlTable(schema_name=mf_test_session_state.mf_system_schema, table_name=_random_table())
    sql_client.create_table_as_select(sql_table, _select_x_as_y())
    assert sql_client.table_exists(sql_table)


def test_percent_signs_in_query(sql_client: SqlClient) -> None:
    """Note: this only syntax works for Datbricks if no execution params are passed."""
    stmt = "SELECT foo FROM ( SELECT 'abba' AS foo ) source0 WHERE foo LIKE '%a'"
    sql_client.query(stmt)
    df = sql_client.query(stmt)
    _check_1col(df, col="foo", vals={"abba"})


@pytest.fixture()
def example_df() -> pd.DataFrame:
    """Data frame containing data of different types for testing. DateTime would be good to add."""
    return pd.DataFrame(
        columns=["int_col", "str_col", "float_col", "bool_col"],
        data=[
            (1, "abc", 1.23, False),
            (2, "def", 4.56, True),
        ],
    )


def test_health_checks(mf_test_session_state: MetricFlowTestSessionState, sql_client: SqlClient) -> None:  # noqa: D
    sql_client.health_checks(schema_name=mf_test_session_state.mf_system_schema)


def test_dry_run(mf_test_session_state: MetricFlowTestSessionState, sql_client: SqlClient) -> None:  # noqa: D
    test_table = SqlTable(schema_name=mf_test_session_state.mf_system_schema, table_name=_random_table())

    stmt = f"CREATE TABLE {test_table.sql} AS SELECT 1 AS foo"
    sql_client.dry_run(stmt)
    assert not sql_client.table_exists(
        test_table
    ), f"Table {test_table.sql} should not exist if the CREATE TABLE was a dry run."


def test_dry_run_of_bad_query_raises_exception(sql_client: SqlClient) -> None:  # noqa: D
    bad_stmt = "SELECT bad_col"
    # Tests that a bad query raises an exception. Different engines may raise different exceptions e.g.
    # ProgrammingError, OperationalError, google.api_core.exceptions.BadRequest, etc.
    with pytest.raises(Exception, match=r"bad_col"):
        sql_client.dry_run(bad_stmt)


def _issue_sleep_query(sql_client: SqlClient, sleep_time: int) -> None:
    """Issue a query that sleeps for a given number of seconds."""
    engine_type = sql_client.sql_engine_attributes.sql_engine_type
    if engine_type is SqlEngine.SNOWFLAKE:
        sql_client.execute(f"CALL system$wait({sleep_time}, 'SECONDS')")
    elif (
        engine_type is SqlEngine.DUCKDB
        or engine_type is SqlEngine.BIGQUERY
        or engine_type is SqlEngine.REDSHIFT
        or engine_type is SqlEngine.DATABRICKS
        or engine_type is SqlEngine.POSTGRES
    ):
        raise RuntimeError(f"Sleep yet not supported with {engine_type}")
    else:
        assert_values_exhausted(engine_type)


def _supports_sleep_query(sql_client: SqlClient) -> bool:
    """Returns true if the given SQL client is supported by _issue_sleep_query()."""
    engine_type = sql_client.sql_engine_attributes.sql_engine_type
    if engine_type is SqlEngine.SNOWFLAKE:
        return True
    elif (
        engine_type is SqlEngine.DUCKDB
        or engine_type is SqlEngine.BIGQUERY
        or engine_type is SqlEngine.REDSHIFT
        or engine_type is SqlEngine.DATABRICKS
        or engine_type is SqlEngine.POSTGRES
    ):
        return False
    else:
        assert_values_exhausted(engine_type)


def test_cancel_submitted_queries(  # noqa: D
    mf_test_session_state: MetricFlowTestSessionState, sql_client: SqlClient
) -> None:
    if not sql_client.sql_engine_attributes.cancel_submitted_queries_supported:
        pytest.skip(
            f"Cancelling queries not yet supported with {sql_client.sql_engine_attributes.sql_engine_type.name}"
        )

    if not _supports_sleep_query(sql_client):
        pytest.skip(f"Sleep queries not yet supported with {sql_client.sql_engine_attributes.sql_engine_type.name}")

    # Submit a 5s sleep query, but then cancel it after 0.5s.
    def cancel_submitted_queries() -> None:
        try:
            sql_client.cancel_submitted_queries()
        except Exception:
            logger.exception("Got an exception while trying to cancel submitted queries.")

    timer_task = threading.Timer(0.5, cancel_submitted_queries)
    timer_task.start()
    with pytest.raises(ProgrammingError):
        if sql_client.sql_engine_attributes.sql_engine_type == SqlEngine.SNOWFLAKE:
            _issue_sleep_query(sql_client, 5)


def test_update_params_with_same_item() -> None:  # noqa: D
    bind_params0 = SqlBindParameters.create_from_dict({"key": "value"})
    bind_params1 = SqlBindParameters.create_from_dict({"key": "value"})

    bind_params0.combine(bind_params1)


def test_update_params_with_same_key_different_values() -> None:  # noqa: D
    bind_params0 = SqlBindParameters.create_from_dict(({"key": "value0"}))
    bind_params1 = SqlBindParameters.create_from_dict(({"key": "value1"}))

    with pytest.raises(RuntimeError):
        bind_params0.combine(bind_params1)


def test_list_tables(mf_test_session_state: MetricFlowTestSessionState, sql_client: SqlClient) -> None:  # noqa: D
    schema = mf_test_session_state.mf_system_schema
    sql_table = SqlTable(schema_name=schema, table_name=_random_table())
    table_count_before_create = len(sql_client.list_tables(schema))
    sql_client.create_table_as_select(sql_table, _select_x_as_y())
    table_list = sql_client.list_tables(schema)
    table_count_after_create = len(table_list)
    assert table_count_after_create == table_count_before_create + 1
    assert len([x for x in table_list if x == sql_table.table_name]) == 1
