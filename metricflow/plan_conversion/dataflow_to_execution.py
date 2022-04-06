import logging
from typing import Generic, Tuple, Optional, Union

from metricflow.protocols.sql_client import SqlClient
from metricflow.dag.id_generation import IdGeneratorRegistry, SQL_QUERY_PLAN_PREFIX, EXEC_PLAN_PREFIX
from metricflow.dataflow.dataflow_plan import (
    SourceDataSetT,
    WriteToResultDataframeNode,
    DataflowPlan,
    SinkNodeVisitor,
    WriteToResultTableNode,
    ComputedMetricsOutput,
    BaseOutput,
)
from metricflow.dataflow.sql_table import SqlTable
from metricflow.execution.execution_plan import (
    ExecutionPlan,
    SelectSqlQueryToDataFrameTask,
    ExecutionPlanTask,
    SelectSqlQueryToTableTask,
)
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter, SqlDataSetT
from metricflow.specs import OutputColumnNameOverride
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql.sql_plan import SqlSelectStatementNode, SqlSelectColumn, SqlQueryPlan
from metricflow.sql.sql_plan_to_text import sql_query_plan_as_text

logger = logging.getLogger(__name__)


class DataflowToExecutionPlanConverter(Generic[SqlDataSetT], SinkNodeVisitor[SqlDataSetT, ExecutionPlan]):
    """Converts a dataflow plan to an execution plan"""

    def __init__(
        self,
        sql_plan_converter: DataflowToSqlQueryPlanConverter[SqlDataSetT],
        sql_plan_renderer: SqlQueryPlanRenderer,
        sql_client: SqlClient,
        output_column_name_overrides: Tuple[OutputColumnNameOverride, ...] = (),
    ) -> None:
        """Constructor.

        Args:
            sql_plan_converter: Converts a dataflow plan node to a SQL query plan
            sql_plan_renderer: Converts a SQL query plan to SQL text
            sql_client: The client to use for running queries.
            output_column_name_overrides: In the output dataframe / table, name output columns in a specific way.
        """
        self._sql_plan_converter = sql_plan_converter
        self._sql_plan_renderer = sql_plan_renderer
        self._sql_client = sql_client
        self._output_column_name_overrides = output_column_name_overrides

    @staticmethod
    def override_output_column_names(
        sql_plan_converter: DataflowToSqlQueryPlanConverter[SqlDataSetT],
        output_column_name_overrides: Tuple[OutputColumnNameOverride, ...],
        select_node: SqlSelectStatementNode,
    ) -> SqlSelectStatementNode:
        """Change the output column names in the select_node according ot output_column_name_overrides."""
        column_name_mapping = {}
        for column_name_override in output_column_name_overrides:
            expected_column_name = sql_plan_converter.column_association_resolver.resolve_time_dimension_spec(
                column_name_override.time_dimension_spec
            ).column_name
            assert expected_column_name in [
                x.column_alias for x in select_node.select_columns
            ], f"Column {expected_column_name} not in {[x.column_alias for x in select_node.select_columns]}"
            new_column_name = column_name_override.output_column_name
            column_name_mapping[expected_column_name] = new_column_name

        new_select_columns = []
        for select_column in select_node.select_columns:
            if select_column.column_alias in column_name_mapping:
                new_select_columns.append(
                    SqlSelectColumn(
                        expr=select_column.expr, column_alias=column_name_mapping[select_column.column_alias]
                    )
                )
            else:
                new_select_columns.append(select_column)

        return SqlSelectStatementNode(
            description=select_node.description,
            select_columns=tuple(new_select_columns),
            from_source=select_node.from_source,
            from_source_alias=select_node.from_source_alias,
            joins_descs=select_node.join_descs,
            group_bys=select_node.group_bys,
            order_bys=select_node.order_bys,
            where=select_node.where,
            limit=select_node.limit,
        )

    def _build_execution_plan(  # noqa: D
        self,
        node: Union[BaseOutput[SourceDataSetT], ComputedMetricsOutput[SourceDataSetT]],
        output_table: Optional[SqlTable] = None,
    ) -> ExecutionPlan:

        sql_plan = self._sql_plan_converter.convert_to_sql_query_plan(
            sql_engine_attributes=self._sql_client.sql_engine_attributes,
            sql_query_plan_id=IdGeneratorRegistry.for_class(SqlQueryPlan).create_id(SQL_QUERY_PLAN_PREFIX),
            dataflow_plan_node=node,
        )

        if self._output_column_name_overrides:
            select_node = sql_plan.render_node.as_select_node
            assert select_node

            sql_plan = SqlQueryPlan(
                plan_id=IdGeneratorRegistry.for_class(SqlQueryPlan).create_id(SQL_QUERY_PLAN_PREFIX),
                render_node=DataflowToExecutionPlanConverter.override_output_column_names(
                    sql_plan_converter=self._sql_plan_converter,
                    output_column_name_overrides=self._output_column_name_overrides,
                    select_node=select_node,
                ),
            )

        logger.debug(f"Generated SQL query plan is:\n{sql_query_plan_as_text(sql_plan)}")

        render_result = self._sql_plan_renderer.render_sql_query_plan(sql_plan)

        leaf_task: ExecutionPlanTask

        if not output_table:
            leaf_task = SelectSqlQueryToDataFrameTask(
                sql_client=self._sql_client,
                sql_query=render_result.sql,
                execution_parameters=render_result.execution_parameters,
            )
        else:
            leaf_task = SelectSqlQueryToTableTask(
                sql_client=self._sql_client,
                sql_query=render_result.sql,
                execution_parameters=render_result.execution_parameters,
                output_table=output_table,
            )

        return ExecutionPlan(
            plan_id=IdGeneratorRegistry.for_class(self.__class__).create_id(EXEC_PLAN_PREFIX), leaf_tasks=[leaf_task]
        )

    def visit_write_to_result_dataframe_node(  # noqa: D
        self, node: WriteToResultDataframeNode[SourceDataSetT]
    ) -> ExecutionPlan:
        logger.info(f"Generating SQL query plan from {node.node_id} -> {node.parent_node.node_id}")
        return self._build_execution_plan(node.parent_node)

    def visit_write_to_result_table_node(  # noqa: D
        self, node: WriteToResultTableNode[SourceDataSetT]
    ) -> ExecutionPlan:
        logger.info(f"Generating SQL query plan from {node.node_id} -> {node.parent_node.node_id}")
        return self._build_execution_plan(node.parent_node, node.output_sql_table)

    def convert_to_execution_plan(self, dataflow_plan: DataflowPlan) -> ExecutionPlan:
        """Convert the dataflow plan to an execution plan."""

        assert len(dataflow_plan.sink_output_nodes) == 1, "Only 1 sink node in the plan is currently supported."
        return dataflow_plan.sink_output_nodes[0].accept_sink_node_visitor(self)
