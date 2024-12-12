from __future__ import annotations

import logging

from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

from metricflow.dataflow.dataflow_plan import (
    DataflowPlan,
    DataflowPlanNode,
)
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor
from metricflow.dataflow.nodes.add_generated_uuid import AddGeneratedUuidColumnNode
from metricflow.dataflow.nodes.aggregate_measures import AggregateMeasuresNode
from metricflow.dataflow.nodes.alias_specs import AliasSpecsNode
from metricflow.dataflow.nodes.combine_aggregated_outputs import CombineAggregatedOutputsNode
from metricflow.dataflow.nodes.compute_metrics import ComputeMetricsNode
from metricflow.dataflow.nodes.constrain_time import ConstrainTimeRangeNode
from metricflow.dataflow.nodes.filter_elements import FilterElementsNode
from metricflow.dataflow.nodes.join_conversion_events import JoinConversionEventsNode
from metricflow.dataflow.nodes.join_over_time import JoinOverTimeRangeNode
from metricflow.dataflow.nodes.join_to_base import JoinOnEntitiesNode
from metricflow.dataflow.nodes.join_to_custom_granularity import JoinToCustomGranularityNode
from metricflow.dataflow.nodes.join_to_time_spine import JoinToTimeSpineNode
from metricflow.dataflow.nodes.metric_time_transform import MetricTimeDimensionTransformNode
from metricflow.dataflow.nodes.min_max import MinMaxNode
from metricflow.dataflow.nodes.order_by_limit import OrderByLimitNode
from metricflow.dataflow.nodes.read_sql_source import ReadSqlSourceNode
from metricflow.dataflow.nodes.semi_additive_join import SemiAdditiveJoinNode
from metricflow.dataflow.nodes.where_filter import WhereConstraintNode
from metricflow.dataflow.nodes.window_reaggregation_node import WindowReaggregationNode
from metricflow.dataflow.nodes.write_to_data_table import WriteToResultDataTableNode
from metricflow.dataflow.nodes.write_to_table import WriteToResultTableNode
from metricflow.execution.convert_to_execution_plan import ConvertToExecutionPlanResult
from metricflow.execution.execution_plan import (
    ExecutionPlan,
    SelectSqlQueryToDataTableTask,
    SelectSqlQueryToTableTask,
    SqlStatement,
)
from metricflow.plan_conversion.convert_to_sql_plan import ConvertToSqlPlanResult
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.optimizer.optimization_levels import SqlQueryOptimizationLevel
from metricflow.sql.render.sql_plan_renderer import SqlPlanRenderResult, SqlQueryPlanRenderer

logger = logging.getLogger(__name__)


class DataflowToExecutionPlanConverter(DataflowPlanNodeVisitor[ConvertToExecutionPlanResult]):
    """Converts a dataflow plan to an execution plan."""

    def __init__(
        self,
        sql_plan_converter: DataflowToSqlQueryPlanConverter,
        sql_plan_renderer: SqlQueryPlanRenderer,
        sql_client: SqlClient,
        sql_optimization_level: SqlQueryOptimizationLevel,
    ) -> None:
        """Constructor.

        Args:
            sql_plan_converter: Converts a dataflow plan node to a SQL query plan
            sql_plan_renderer: Converts a SQL query plan to SQL text
            sql_client: The client to use for running queries.
            sql_optimization_level: The optimization level to use for generating the SQL.
        """
        self._sql_plan_converter = sql_plan_converter
        self._sql_plan_renderer = sql_plan_renderer
        self._sql_client = sql_client
        self._optimization_level = sql_optimization_level

    def _convert_to_sql_plan(self, node: DataflowPlanNode) -> ConvertToSqlPlanResult:
        logger.debug(LazyFormat(lambda: f"Generating SQL query plan from {node.node_id}"))
        result = self._sql_plan_converter.convert_to_sql_query_plan(
            sql_engine_type=self._sql_client.sql_engine_type,
            optimization_level=self._optimization_level,
            dataflow_plan_node=node,
        )
        logger.debug(LazyFormat(lambda: f"Generated SQL query plan is:\n{result.sql_plan.structure_text()}"))
        return result

    def _render_sql(self, convert_to_sql_plan_result: ConvertToSqlPlanResult) -> SqlPlanRenderResult:
        return self._sql_plan_renderer.render_sql_query_plan(convert_to_sql_plan_result.sql_plan)

    @override
    def visit_write_to_result_data_table_node(self, node: WriteToResultDataTableNode) -> ConvertToExecutionPlanResult:
        convert_to_sql_plan_result = self._convert_to_sql_plan(node)
        render_sql_result = self._render_sql(convert_to_sql_plan_result)
        execution_plan = ExecutionPlan(
            leaf_tasks=(
                SelectSqlQueryToDataTableTask.create(
                    sql_client=self._sql_client,
                    sql_statement=SqlStatement(render_sql_result.sql, render_sql_result.bind_parameter_set),
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
                SelectSqlQueryToTableTask.create(
                    sql_client=self._sql_client,
                    sql_statement=SqlStatement(
                        sql=render_sql_result.sql,
                        bind_parameter_set=render_sql_result.bind_parameter_set,
                    ),
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
        assert len(dataflow_plan.sink_nodes) == 1, "Only 1 sink node in the plan is currently supported."
        return dataflow_plan.sink_nodes[0].accept(self)

    @override
    def visit_source_node(self, node: ReadSqlSourceNode) -> ConvertToExecutionPlanResult:
        raise NotImplementedError

    @override
    def visit_join_on_entities_node(self, node: JoinOnEntitiesNode) -> ConvertToExecutionPlanResult:
        raise NotImplementedError

    @override
    def visit_aggregate_measures_node(self, node: AggregateMeasuresNode) -> ConvertToExecutionPlanResult:
        raise NotImplementedError

    @override
    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> ConvertToExecutionPlanResult:
        raise NotImplementedError

    @override
    def visit_window_reaggregation_node(self, node: WindowReaggregationNode) -> ConvertToExecutionPlanResult:
        raise NotImplementedError

    @override
    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> ConvertToExecutionPlanResult:
        raise NotImplementedError

    @override
    def visit_where_constraint_node(self, node: WhereConstraintNode) -> ConvertToExecutionPlanResult:
        raise NotImplementedError

    @override
    def visit_filter_elements_node(self, node: FilterElementsNode) -> ConvertToExecutionPlanResult:
        raise NotImplementedError

    @override
    def visit_combine_aggregated_outputs_node(self, node: CombineAggregatedOutputsNode) -> ConvertToExecutionPlanResult:
        raise NotImplementedError

    @override
    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode) -> ConvertToExecutionPlanResult:
        raise NotImplementedError

    @override
    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode) -> ConvertToExecutionPlanResult:
        raise NotImplementedError

    @override
    def visit_semi_additive_join_node(self, node: SemiAdditiveJoinNode) -> ConvertToExecutionPlanResult:
        raise NotImplementedError

    @override
    def visit_metric_time_dimension_transform_node(
        self, node: MetricTimeDimensionTransformNode
    ) -> ConvertToExecutionPlanResult:
        raise NotImplementedError

    @override
    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode) -> ConvertToExecutionPlanResult:
        raise NotImplementedError

    @override
    def visit_min_max_node(self, node: MinMaxNode) -> ConvertToExecutionPlanResult:
        raise NotImplementedError

    @override
    def visit_add_generated_uuid_column_node(self, node: AddGeneratedUuidColumnNode) -> ConvertToExecutionPlanResult:
        raise NotImplementedError

    @override
    def visit_join_conversion_events_node(self, node: JoinConversionEventsNode) -> ConvertToExecutionPlanResult:
        raise NotImplementedError

    @override
    def visit_join_to_custom_granularity_node(self, node: JoinToCustomGranularityNode) -> ConvertToExecutionPlanResult:
        raise NotImplementedError

    @override
    def visit_alias_specs_node(self, node: AliasSpecsNode) -> ConvertToExecutionPlanResult:
        raise NotImplementedError
