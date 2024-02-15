from __future__ import annotations

import logging
from typing import Optional, Union

from metricflow.dataflow.dataflow_plan import (
    BaseOutput,
    ComputedMetricsOutput,
    DataflowPlan,
    SinkNodeVisitor,
    WriteToResultDataframeNode,
    WriteToResultTableNode,
)
from metricflow.dataflow.sql_table import SqlTable
from metricflow.execution.execution_plan import (
    ExecutionPlan,
    ExecutionPlanTask,
    SelectSqlQueryToDataFrameTask,
    SelectSqlQueryToTableTask,
)
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer

logger = logging.getLogger(__name__)


class DataflowToExecutionPlanConverter(SinkNodeVisitor[ExecutionPlan]):
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

    def _build_execution_plan(  # noqa: D
        self,
        node: Union[BaseOutput, ComputedMetricsOutput],
        output_table: Optional[SqlTable] = None,
    ) -> ExecutionPlan:
        sql_plan = self._sql_plan_converter.convert_to_sql_query_plan(
            sql_engine_type=self._sql_client.sql_engine_type,
            dataflow_plan_node=node,
        )

        logger.debug(f"Generated SQL query plan is:\n{sql_plan.text_structure()}")

        render_result = self._sql_plan_renderer.render_sql_query_plan(sql_plan)

        leaf_task: ExecutionPlanTask

        if not output_table:
            leaf_task = SelectSqlQueryToDataFrameTask(
                sql_client=self._sql_client,
                sql_query=render_result.sql,
                bind_parameters=render_result.bind_parameters,
            )
        else:
            leaf_task = SelectSqlQueryToTableTask(
                sql_client=self._sql_client,
                sql_query=render_result.sql,
                bind_parameters=render_result.bind_parameters,
                output_table=output_table,
            )

        return ExecutionPlan(
            leaf_tasks=[leaf_task],
        )

    def visit_write_to_result_dataframe_node(self, node: WriteToResultDataframeNode) -> ExecutionPlan:  # noqa: D
        logger.info(f"Generating SQL query plan from {node.node_id} -> {node.parent_node.node_id}")
        return self._build_execution_plan(node.parent_node)

    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> ExecutionPlan:  # noqa: D
        logger.info(f"Generating SQL query plan from {node.node_id} -> {node.parent_node.node_id}")
        return self._build_execution_plan(node.parent_node, node.output_sql_table)

    def convert_to_execution_plan(self, dataflow_plan: DataflowPlan) -> ExecutionPlan:
        """Convert the dataflow plan to an execution plan."""
        assert len(dataflow_plan.sink_output_nodes) == 1, "Only 1 sink node in the plan is currently supported."
        return dataflow_plan.sink_output_nodes[0].accept_sink_node_visitor(self)
