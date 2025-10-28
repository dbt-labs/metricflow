from __future__ import annotations

from metricflow_semantics.dag.mf_dag import DagId
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.toolkit.random_id import mf_random_id

from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.execution.execution_plan import (
    ExecutionPlan,
    SelectSqlQueryToDataTableTask,
    SelectSqlQueryToTableTask,
    SqlStatement,
)
from metricflow.execution.executor import SequentialPlanExecutor
from metricflow.protocols.sql_client import SqlClient, SqlEngine
from tests_metricflow.sql.compare_data_table import assert_data_tables_equal


def test_read_sql_task(sql_client: SqlClient) -> None:  # noqa: D103
    task = SelectSqlQueryToDataTableTask.create(sql_client, SqlStatement("SELECT 1 AS foo", SqlBindParameterSet()))
    execution_plan = ExecutionPlan(leaf_tasks=[task], dag_id=DagId.from_str("plan0"))

    results = SequentialPlanExecutor().execute_plan(execution_plan)
    task_result = results.get_result(task.task_id)

    assert not results.contains_task_errors
    assert task_result.df is not None

    assert_data_tables_equal(
        actual=task_result.df,
        expected=MetricFlowDataTable.create_from_rows(
            column_names=["foo"],
            rows=[(1,)],
        ),
        compare_names_using_lowercase=sql_client.sql_engine_type is SqlEngine.SNOWFLAKE,
    )


def test_write_table_task(  # noqa: D103
    mf_test_configuration: MetricFlowTestConfiguration, sql_client: SqlClient
) -> None:  # noqa: D103
    output_table = SqlTable(
        schema_name=mf_test_configuration.mf_system_schema, table_name=f"test_table_{mf_random_id()}"
    )
    task = SelectSqlQueryToTableTask.create(
        sql_client=sql_client,
        sql_statement=SqlStatement(
            sql=f"CREATE TABLE {output_table.sql} AS SELECT 1 AS foo",
            bind_parameter_set=SqlBindParameterSet(),
        ),
        output_table=output_table,
    )
    execution_plan = ExecutionPlan(leaf_tasks=[task], dag_id=DagId.from_str("plan0"))

    results = SequentialPlanExecutor().execute_plan(execution_plan)

    assert not results.contains_task_errors

    assert_data_tables_equal(
        actual=sql_client.query(f"SELECT * FROM {output_table.sql}"),
        expected=MetricFlowDataTable.create_from_rows(
            column_names=["foo"],
            rows=[(1,)],
        ),
        compare_names_using_lowercase=sql_client.sql_engine_type is SqlEngine.SNOWFLAKE,
    )
    sql_client.execute(f"DROP TABLE IF EXISTS {output_table.sql}")
