from __future__ import annotations

import datetime
import logging
from typing import Sequence, Set, Union

import pandas as pd
import pytest

from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SqlClient, SqlEngine
from metricflow.random_id import random_id
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql.sql_column_type import SqlColumnType
from metricflow.test.compare_df import assert_dataframes_equal
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.fixtures.sql_clients.ddl_sql_client import SqlClientWithDDLMethods

logger = logging.getLogger(__name__)


def _random_table() -> str:
    return f"test_table_{random_id()}"


def _select_x_as_y(x: int = 1, y: str = "y") -> str:  # noqa: D
    return f"SELECT {x} AS {y}"


def _check_1col(df: pd.DataFrame, col: str = "y", vals: Set[Union[int, str]] = {1}) -> None:
    """Helper to check that 1 column has the same value and a case-insensitive matching name.

    We lower-case the names due to snowflake's tendency to capitalize things. This isn't ideal but it'll do for now.
    """
    df.columns = df.columns.str.lower()
    col = col.lower()
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (len(vals), 1)
    assert df.columns.tolist() == [col]
    assert set(df[col]) == vals


def test_query(sql_client: SqlClient) -> None:  # noqa: D
    df = sql_client.query(_select_x_as_y())
    _check_1col(df)


def _skip_execution_param_tests_for_unsupported_clients(sql_client: SqlClient) -> None:
    if sql_client.sql_engine_type is not SqlEngine.DUCKDB:
        pytest.skip(
            reason=(
                "The dbt Adapter-backed SqlClient implementation does not support bind parameters, so we restrict "
                "this test to our DuckDB client, which retains an example implementation."
            )
        )


def test_query_with_execution_params(sql_client: SqlClient) -> None:
    """Test querying with execution parameters of all supported datatypes."""
    _skip_execution_param_tests_for_unsupported_clients(sql_client)
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
    _skip_execution_param_tests_for_unsupported_clients(sql_client)
    expr = f"SELECT {sql_client.render_bind_parameter_key('x')}"
    sql_execution_params = SqlBindParameters.create_from_dict({"x": 1})

    sql_client.query(expr, sql_bind_parameters=sql_execution_params)
    with pytest.raises(Exception):
        sql_client.query("this is garbage")


def test_create_table_from_dataframe(  # noqa: D
    mf_test_session_state: MetricFlowTestSessionState, ddl_sql_client: SqlClientWithDDLMethods
) -> None:
    expected_df = pd.DataFrame(
        columns=["int_col", "str_col", "float_col", "bool_col", "time_col"],
        data=[
            (1, "abc", 1.23, False, "2020-01-01"),
            (2, "def", 4.56, True, "2020-01-02"),
            (3, "ghi", 1.1, False, None),  # Test NaT type
            (None, "jkl", None, True, "2020-01-03"),  # Test NaN types
            (3, None, 1.2, None, "2020-01-04"),  # Test None types for NA conversions
        ],
    )
    sql_table = SqlTable(schema_name=mf_test_session_state.mf_system_schema, table_name=_random_table())
    ddl_sql_client.create_table_from_dataframe(sql_table=sql_table, df=expected_df)

    actual_df = ddl_sql_client.query(f"SELECT * FROM {sql_table.sql}")
    assert_dataframes_equal(
        actual=actual_df,
        expected=expected_df,
        compare_names_using_lowercase=ddl_sql_client.sql_engine_type is SqlEngine.SNOWFLAKE,
    )


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


def test_dry_run(mf_test_session_state: MetricFlowTestSessionState, sql_client: SqlClient) -> None:  # noqa: D
    test_table = SqlTable(schema_name=mf_test_session_state.mf_system_schema, table_name=_random_table())

    stmt = f"CREATE TABLE {test_table.sql} AS SELECT 1 AS foo"
    sql_client.dry_run(stmt)
    with pytest.raises(expected_exception=Exception) as excinfo:
        sql_client.execute(stmt=f"SELECT * FROM {test_table.sql}")

    exception_message = repr(excinfo.value)
    match = (
        test_table.table_name
        if sql_client.sql_engine_type is not SqlEngine.SNOWFLAKE
        else str.upper(test_table.table_name)
    )
    assert (
        exception_message.find(f"{match}") != -1
    ), f"Expected an exception about table {match} not found, but got `{exception_message}`"


def test_dry_run_of_bad_query_raises_exception(sql_client: SqlClient) -> None:  # noqa: D
    bad_stmt = "SELECT bad_col"
    # Tests that a bad query raises an exception. Different engines may raise different exceptions e.g.
    # ProgrammingError, OperationalError, google.api_core.exceptions.BadRequest, etc.
    with pytest.raises(Exception, match=r"(?i)bad_col"):
        sql_client.dry_run(bad_stmt)


def test_update_params_with_same_item() -> None:  # noqa: D
    bind_params0 = SqlBindParameters.create_from_dict({"key": "value"})
    bind_params1 = SqlBindParameters.create_from_dict({"key": "value"})

    bind_params0.combine(bind_params1)


def test_update_params_with_same_key_different_values() -> None:  # noqa: D
    bind_params0 = SqlBindParameters.create_from_dict(({"key": "value0"}))
    bind_params1 = SqlBindParameters.create_from_dict(({"key": "value1"}))

    with pytest.raises(RuntimeError):
        bind_params0.combine(bind_params1)
