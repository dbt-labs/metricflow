from __future__ import annotations

import logging

from typing_extensions import override

from metricflow.dataflow.dataflow_plan import (
    DataflowPlan,
    DataflowPlanNode,
    SinkNodeVisitor,
)
from metricflow.dataflow.nodes.write_to_dataframe import WriteToResultDataframeNode
from metricflow.dataflow.nodes.write_to_table import WriteToResultTableNode
from metricflow.execution.execution_plan import (
    ExecutionPlan,
    SelectSqlQueryToDataFrameTask,
    SelectSqlQueryToTableTask,
)
from metricflow.plan_conversion.convert_to_execution_plan import ConvertToExecutionPlanResult
from metricflow.plan_conversion.convert_to_sql_plan import ConvertToSqlPlanResult
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.render.sql_plan_renderer import SqlPlanRenderResult, SqlQueryPlanRenderer

logger = logging.getLogger(__name__)


class DataflowToExecutionPlanConverter(SinkNodeVisitor[ConvertToExecutionPlanResult]):
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
        assert len(dataflow_plan.sink_output_nodes) == 1, "Only 1 sink node in the plan is currently supported."
        return dataflow_plan.sink_output_nodes[0].accept_sink_node_visitor(self)
