from __future__ import annotations

import logging
from typing import Optional, Set, Union

import pytest
from dbt_semantic_interfaces.test_utils import as_datetime
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.toolkit.id_helpers import mf_random_id

from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.protocols.sql_client import SqlClient, SqlEngine
from tests_metricflow.fixtures.sql_clients.ddl_sql_client import SqlClientWithDDLMethods
from tests_metricflow.sql.compare_data_table import assert_data_tables_equal

logger = logging.getLogger(__name__)


def _random_table() -> str:
    return f"test_table_{mf_random_id()}"


def _select_x_as_y(x: int = 1, y: str = "y") -> str:
    return f"SELECT {x} AS {y}"


def _check_1col(df: MetricFlowDataTable, col: str = "y", vals: Optional[Set[Union[int, str]]] = None) -> None:
    """Helper to check that 1 column has the same value and a case-insensitive matching name.

    We lower-case the names due to snowflake's tendency to capitalize things. This isn't ideal but it'll do for now.
    """
    if vals is None:
        vals = {1}

    assert df.column_count == 1
    assert tuple(column_name.lower() for column_name in df.column_names) == (col.lower(),)
    assert set(df.column_values_iterator(0)) == vals


def test_query(sql_client: SqlClient) -> None:  # noqa: D103
    df = sql_client.query(_select_x_as_y())
    _check_1col(df)


def test_select_one_query(sql_client: SqlClient) -> None:  # noqa: D103
    sql_client.query("SELECT 1")
    with pytest.raises(Exception):
        sql_client.query("this is garbage")


def test_create_table_from_data_table(  # noqa: D103
    mf_test_configuration: MetricFlowTestConfiguration, ddl_sql_client: SqlClientWithDDLMethods
) -> None:
    expected_df = MetricFlowDataTable.create_from_rows(
        column_names=["int_col", "str_col", "float_col", "bool_col", "time_col"],
        rows=[
            (1, "abc", 1.23, False, as_datetime("2020-01-01")),
            (2, "def", 4.56, True, as_datetime("2020-01-02")),
            (3, "ghi", 1.1, False, None),  # Test NaT type
            (None, "jkl", None, True, as_datetime("2020-01-03")),  # Test NaN types
            (3, None, 1.2, None, as_datetime("2020-01-04")),  # Test None types for NA conversions
        ],
    )
    sql_table = SqlTable(schema_name=mf_test_configuration.mf_system_schema, table_name=_random_table())
    ddl_sql_client.create_table_from_data_table(sql_table=sql_table, df=expected_df)

    actual_df = ddl_sql_client.query(f"SELECT * FROM {sql_table.sql}")
    assert_data_tables_equal(
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
def example_df() -> MetricFlowDataTable:
    """Data frame containing data of different types for testing. DateTime would be good to add."""
    return MetricFlowDataTable.create_from_rows(
        column_names=["int_col", "str_col", "float_col", "bool_col"],
        rows=[
            (1, "abc", 1.23, False),
            (2, "def", 4.56, True),
        ],
    )


def test_dry_run(mf_test_configuration: MetricFlowTestConfiguration, sql_client: SqlClient) -> None:  # noqa: D103
    test_table = SqlTable(schema_name=mf_test_configuration.mf_system_schema, table_name=_random_table())

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


def test_dry_run_of_bad_query_raises_exception(sql_client: SqlClient) -> None:  # noqa: D103
    bad_stmt = "SELECT bad_col"
    # Tests that a bad query raises an exception. Different engines may raise different exceptions e.g.
    # ProgrammingError, OperationalError, google.api_core.exceptions.BadRequest, etc.
    with pytest.raises(Exception, match=r"(?i)bad_col"):
        sql_client.dry_run(bad_stmt)


def test_update_params_with_same_item() -> None:  # noqa: D103
    bind_params0 = SqlBindParameterSet.create_from_dict({"key": "value"})
    bind_params1 = SqlBindParameterSet.create_from_dict({"key": "value"})

    bind_params0.merge(bind_params1)


def test_update_params_with_same_key_different_values() -> None:  # noqa: D103
    bind_params0 = SqlBindParameterSet.create_from_dict(({"key": "value0"}))
    bind_params1 = SqlBindParameterSet.create_from_dict(({"key": "value1"}))

    with pytest.raises(RuntimeError):
        bind_params0.merge(bind_params1)
