from __future__ import annotations

from metricflow.dataflow.sql_table import SqlTable
from metricflow.execution.execution_plan import (
    ExecutionPlan,
    SelectSqlQueryToDataFrameTask,
    SelectSqlQueryToTableTask,
)
from metricflow.execution.executor import SequentialPlanExecutor
from metricflow.protocols.async_sql_client import AsyncSqlClient
from metricflow.random_id import random_id
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql_clients.sql_utils import make_df
from metricflow.test.compare_df import assert_dataframes_equal
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState


def test_read_sql_task(async_sql_client: AsyncSqlClient) -> None:  # noqa: D
    task = SelectSqlQueryToDataFrameTask(async_sql_client, "SELECT 1 AS foo", SqlBindParameters())
    execution_plan = ExecutionPlan("plan0", leaf_tasks=[task])

    results = SequentialPlanExecutor().execute_plan(execution_plan)
    task_result = results.get_result(task.task_id)

    assert not results.contains_task_errors
    assert task_result.df is not None

    assert_dataframes_equal(
        actual=task_result.df,
        expected=make_df(
            sql_client=async_sql_client,
            columns=["foo"],
            data=[(1,)],
        ),
    )


def test_write_table_task(  # noqa: D
    mf_test_session_state: MetricFlowTestSessionState, async_sql_client: AsyncSqlClient
) -> None:
    output_table = SqlTable(schema_name=mf_test_session_state.mf_system_schema, table_name=f"test_table_{random_id()}")
    task = SelectSqlQueryToTableTask(
        sql_client=async_sql_client,
        sql_query="SELECT 1 AS foo",
        bind_parameters=SqlBindParameters(),
        output_table=output_table,
    )
    execution_plan = ExecutionPlan("plan0", leaf_tasks=[task])

    results = SequentialPlanExecutor().execute_plan(execution_plan)

    assert not results.contains_task_errors
    assert async_sql_client.table_exists(output_table)

    assert_dataframes_equal(
        actual=async_sql_client.query(f"SELECT * FROM {output_table.sql}"),
        expected=make_df(
            sql_client=async_sql_client,
            columns=["foo"],
            data=[(1,)],
        ),
    )
    async_sql_client.drop_table(output_table)
