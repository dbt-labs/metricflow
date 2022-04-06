from collections import OrderedDict
from typing import Set, Union

import pandas as pd
import pytest

from metricflow.dataflow.sql_table import SqlTable
from metricflow.object_utils import random_id
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql_clients.sql_utils import make_df
from metricflow.test.compare_df import assert_dataframes_equal
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState


def _random_table() -> str:
    return f"test_table_{random_id()}"


def _select_x_as_y(sql_client: SqlClient, x: int = 1, y: str = "y") -> str:  # noqa: D
    return f"SELECT {x} AS {y}"


def _check_1col(df: pd.DataFrame, col: str = "y", vals: Set[Union[int, str]] = {1}) -> None:  # noqa: D
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (len(vals), 1)
    assert df.columns.tolist() == [col]
    assert set(df[col]) == vals


def test_query(sql_client: SqlClient) -> None:  # noqa: D
    df = sql_client.query(_select_x_as_y(sql_client))
    _check_1col(df)


def test_query_with_execution_params(sql_client: SqlClient) -> None:  # noqa: D
    expr = "SELECT :x as y"
    sql_execution_params = SqlBindParameters()
    sql_execution_params.param_dict = OrderedDict([("x", "1")])
    df = sql_client.query(expr, sql_bind_parameters=sql_execution_params)
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 1)
    assert df.columns.tolist() == ["y"]
    assert set(df["y"]) == {"1"}


def test_select_one_query(sql_client: SqlClient) -> None:  # noqa: D
    sql_client.query("SELECT 1")
    with pytest.raises(Exception):
        sql_client.query("this is garbage")


def test_failed_query_with_execution_params(sql_client: SqlClient) -> None:  # noqa: D
    expr = "SELECT :x"
    sql_execution_params = SqlBindParameters()
    sql_execution_params.param_dict = OrderedDict([("x", "1")])

    sql_client.query(expr, sql_bind_parameters=sql_execution_params)
    with pytest.raises(Exception):
        sql_client.query("this is garbage")


def test_create_table(mf_test_session_state: MetricFlowTestSessionState, sql_client: SqlClient) -> None:  # noqa: D
    sql_table = SqlTable(schema_name=mf_test_session_state.mf_source_schema, table_name=_random_table())
    sql_client.create_table_as_select(sql_table, _select_x_as_y(sql_client))
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
        ],
    )
    sql_table = SqlTable(schema_name=mf_test_session_state.mf_source_schema, table_name=_random_table())
    sql_client.create_table_from_dataframe(sql_table=sql_table, df=expected_df)

    actual_df = sql_client.query(f"SELECT * FROM {sql_table.sql}")
    assert_dataframes_equal(actual=actual_df, expected=expected_df)


def test_table_exists(mf_test_session_state: MetricFlowTestSessionState, sql_client: SqlClient) -> None:  # noqa: D
    sql_table = SqlTable(schema_name=mf_test_session_state.mf_source_schema, table_name=_random_table())
    sql_client.create_table_as_select(sql_table, _select_x_as_y(sql_client))
    assert sql_client.table_exists(sql_table)


def test_percent_signs_in_query(sql_client: SqlClient) -> None:  # noqa: D
    stmt = "SELECT foo FROM ( SELECT 'abba' AS foo ) WHERE foo LIKE '%a'"
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
    sql_client.health_checks(schema_name=mf_test_session_state.mf_source_schema)
