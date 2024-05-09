from __future__ import annotations

import logging
from typing import Sequence

from typing_extensions import override

from metricflow.dataflow.dataflow_plan import (
    DataflowPlan,
    DataflowPlanNode,
)
from metricflow.dataflow.dfs_walker import DataflowDagWalker
from metricflow.dataflow.nodes.write_to_dataframe import WriteToResultDataframeNode
from metricflow.dataflow.nodes.write_to_table import WriteToResultTableNode
from metricflow.execution.convert_to_execution_plan import ConvertToExecutionPlanResult
from metricflow.execution.execution_plan import (
    ExecutionPlan,
    SelectSqlQueryToDataFrameTask,
    SelectSqlQueryToTableTask,
)
from metricflow.plan_conversion.convert_to_sql_plan import ConvertToSqlPlanResult
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.render.sql_plan_renderer import SqlPlanRenderResult, SqlQueryPlanRenderer

logger = logging.getLogger(__name__)


class DataflowToExecutionPlanConverter(DataflowDagWalker[ConvertToExecutionPlanResult]):
    """Converts a dataflow plan to an execution plan."""

    def __init__(
        self,
        sql_plan_converter: DataflowToSqlQueryPlanConverter,
        sql_plan_renderer: SqlQueryPlanRenderer,
        sql_client: SqlClient,
    ) -> None:
        """Constructor.

        Args:
            sql_plan_converter: Converts a dataflow plan node to a SQL query plan
            sql_plan_renderer: Converts a SQL query plan to SQL text
            sql_client: The client to use for running queries.
        """
        self._sql_plan_converter = sql_plan_converter
        self._sql_plan_renderer = sql_plan_renderer
        self._sql_client = sql_client
        super().__init__(visit_log_level=logging.DEBUG, default_action_recursion=True)

    def _convert_to_sql_plan(self, node: DataflowPlanNode) -> ConvertToSqlPlanResult:
        logger.info(f"Generating SQL query plan from {node.node_id}")
        result = self._sql_plan_converter.convert_to_sql_query_plan(
            sql_engine_type=self._sql_client.sql_engine_type,
            dataflow_plan_node=node,
        )
        logger.debug(f"Generated SQL query plan is:\n{result.sql_plan.structure_text()}")
        return result

    def _render_sql(self, convert_to_sql_plan_result: ConvertToSqlPlanResult) -> SqlPlanRenderResult:
        return self._sql_plan_renderer.render_sql_query_plan(convert_to_sql_plan_result.sql_plan)

    @override
    def visit_write_to_result_dataframe_node(self, node: WriteToResultDataframeNode) -> ConvertToExecutionPlanResult:
        convert_to_sql_plan_result = self._convert_to_sql_plan(node)
        render_sql_result = self._render_sql(convert_to_sql_plan_result)
        execution_plan = ExecutionPlan(
            leaf_tasks=(
                SelectSqlQueryToDataFrameTask(
                    sql_client=self._sql_client,
                    sql_query=render_sql_result.sql,
                    bind_parameters=render_sql_result.bind_parameters,
                ),
            )
        )
        return ConvertToExecutionPlanResult(
            convert_to_sql_plan_result=convert_to_sql_plan_result,
            render_sql_result=render_sql_result,
            execution_plan=execution_plan,
        )

    @override
    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> ConvertToExecutionPlanResult:
        convert_to_sql_plan_result = self._convert_to_sql_plan(node)
        render_sql_result = self._render_sql(convert_to_sql_plan_result)
        execution_plan = ExecutionPlan(
            leaf_tasks=(
                SelectSqlQueryToTableTask(
                    sql_client=self._sql_client,
                    sql_query=render_sql_result.sql,
                    bind_parameters=render_sql_result.bind_parameters,
                    output_table=node.output_sql_table,
                ),
            ),
        )
        return ConvertToExecutionPlanResult(
            convert_to_sql_plan_result=convert_to_sql_plan_result,
            render_sql_result=render_sql_result,
            execution_plan=execution_plan,
        )

    def convert_to_execution_plan(self, dataflow_plan: DataflowPlan) -> ConvertToExecutionPlanResult:
        """Convert the dataflow plan to an execution plan."""
        return dataflow_plan.checked_sink_node.accept(self)

    @override
    def default_visit_action(
        self, current_node: DataflowPlanNode, inputs: Sequence[ConvertToExecutionPlanResult]
    ) -> ConvertToExecutionPlanResult:
        raise NotImplementedError(f"{current_node} is not handled")
