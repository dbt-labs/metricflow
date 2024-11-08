from __future__ import annotations

import logging
from typing import Callable, Dict, FrozenSet, List, Sequence, TypeVar

from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.sql.sql_table import SqlTable
from typing_extensions import override

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.nodes.add_generated_uuid import AddGeneratedUuidColumnNode
from metricflow.dataflow.nodes.aggregate_measures import AggregateMeasuresNode
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
from metricflow.dataset.sql_dataset import SqlDataSet
from metricflow.plan_conversion.dataflow_to_sql_subquery import DataflowNodeToSqlSubqueryVisitor
from metricflow.plan_conversion.instance_converters import CreateSelectColumnsForInstances
from metricflow.sql.sql_plan import (
    SqlCteNode,
    SqlSelectStatementNode,
    SqlTableNode,
)

logger = logging.getLogger(__name__)


DataflowNodeT = TypeVar("DataflowNodeT", bound=DataflowPlanNode)


class DataflowNodeToSqlCteVisitor(DataflowNodeToSqlSubqueryVisitor):
    """Similar to `DataflowNodeToSqlSubqueryVisitor`, except that this converts specific nodes to CTEs.

    This is implemented as a subclass of `DataflowNodeToSqlSubqueryVisitor` so that by default, it has the same behavior
    but in cases where there are nodes that should be converted to CTEs, alternate methods can be used.

    The generated CTE nodes are collected instead of getting incorporated into the associated SQL query plan generated
    at each node so that the CTE nodes can be included at the top-level SELECT statement.
    """

    def __init__(  # noqa: D107
        self,
        column_association_resolver: ColumnAssociationResolver,
        semantic_manifest_lookup: SemanticManifestLookup,
        nodes_to_convert_to_cte: FrozenSet[DataflowPlanNode],
    ) -> None:
        super().__init__(
            column_association_resolver=column_association_resolver, semantic_manifest_lookup=semantic_manifest_lookup
        )
        self._nodes_to_convert_to_cte = nodes_to_convert_to_cte
        self._generated_cte_nodes: List[SqlCteNode] = []
        self._generated_cte_data_sets: Dict[DataflowPlanNode, SqlDataSet] = {}

    def generated_cte_nodes(self) -> Sequence[SqlCteNode]:
        """Returns the CTE nodes that have been generated while traversing the dataflow plan."""
        return self._generated_cte_nodes

    def _default_handler(
        self, node: DataflowNodeT, function_to_convert_to_subquery: Callable[[DataflowNodeT], SqlDataSet]
    ) -> SqlDataSet:
        cte_data_set = self._generated_cte_data_sets.get(node)
        if cte_data_set is not None:
            logger.debug(LazyFormat("Handling node via existing CTE", node=node))
            return cte_data_set

        subquery_sql_dataset = function_to_convert_to_subquery(node)

        if node not in self._nodes_to_convert_to_cte:
            logger.debug(LazyFormat("Handling node via subquery", node=node))
            return subquery_sql_dataset
        logger.debug(LazyFormat("Handling node via new CTE", node=node))

        cte_alias = node.node_id.id_str + "_cte"

        if cte_alias in set(node.cte_alias for node in self._generated_cte_nodes):
            raise ValueError(
                f"{cte_alias=} is a duplicate of one that already exists. "
                f"This implies a bug in generating a new CTE for the same dataflow plan node multiple times."
            )

        cte_source = SqlCteNode.create(
            select_statement=subquery_sql_dataset.sql_node,
            cte_alias=cte_alias,
        )
        self._generated_cte_nodes.append(cte_source)
        node_id = node.node_id
        cte_data_set = SqlDataSet(
            instance_set=subquery_sql_dataset.instance_set,
            sql_select_node=SqlSelectStatementNode.create(
                description=f"Read From CTE For {node_id=}",
                select_columns=CreateSelectColumnsForInstances(
                    table_alias=cte_alias,
                    column_resolver=self._column_association_resolver,
                )
                .transform(subquery_sql_dataset.instance_set)
                .as_tuple(),
                from_source=SqlTableNode.create(SqlTable(schema_name=None, table_name=cte_alias)),
                from_source_alias=cte_alias,
            ),
        )
        self._generated_cte_data_sets[node] = cte_data_set

        return cte_data_set

    @override
    def visit_source_node(self, node: ReadSqlSourceNode) -> SqlDataSet:
        return self._default_handler(node=node, function_to_convert_to_subquery=super().visit_source_node)

    @override
    def visit_join_on_entities_node(self, node: JoinOnEntitiesNode) -> SqlDataSet:
        return self._default_handler(node=node, function_to_convert_to_subquery=super().visit_join_on_entities_node)

    @override
    def visit_aggregate_measures_node(self, node: AggregateMeasuresNode) -> SqlDataSet:
        return self._default_handler(node=node, function_to_convert_to_subquery=super().visit_aggregate_measures_node)

    @override
    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> SqlDataSet:
        return self._default_handler(node=node, function_to_convert_to_subquery=super().visit_compute_metrics_node)

    @override
    def visit_window_reaggregation_node(self, node: WindowReaggregationNode) -> SqlDataSet:
        return self._default_handler(node=node, function_to_convert_to_subquery=super().visit_window_reaggregation_node)

    @override
    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> SqlDataSet:
        return self._default_handler(node=node, function_to_convert_to_subquery=super().visit_order_by_limit_node)

    @override
    def visit_where_constraint_node(self, node: WhereConstraintNode) -> SqlDataSet:
        return self._default_handler(node=node, function_to_convert_to_subquery=super().visit_where_constraint_node)

    @override
    def visit_write_to_result_data_table_node(self, node: WriteToResultDataTableNode) -> SqlDataSet:
        return self._default_handler(
            node=node, function_to_convert_to_subquery=super().visit_write_to_result_data_table_node
        )

    @override
    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> SqlDataSet:
        return self._default_handler(
            node=node, function_to_convert_to_subquery=super().visit_write_to_result_table_node
        )

    @override
    def visit_filter_elements_node(self, node: FilterElementsNode) -> SqlDataSet:
        return self._default_handler(node=node, function_to_convert_to_subquery=super().visit_filter_elements_node)

    @override
    def visit_combine_aggregated_outputs_node(self, node: CombineAggregatedOutputsNode) -> SqlDataSet:
        return self._default_handler(
            node=node, function_to_convert_to_subquery=super().visit_combine_aggregated_outputs_node
        )

    @override
    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode) -> SqlDataSet:
        return self._default_handler(node=node, function_to_convert_to_subquery=super().visit_constrain_time_range_node)

    @override
    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode) -> SqlDataSet:
        return self._default_handler(node=node, function_to_convert_to_subquery=super().visit_join_over_time_range_node)

    @override
    def visit_semi_additive_join_node(self, node: SemiAdditiveJoinNode) -> SqlDataSet:
        return self._default_handler(node=node, function_to_convert_to_subquery=super().visit_semi_additive_join_node)

    @override
    def visit_metric_time_dimension_transform_node(self, node: MetricTimeDimensionTransformNode) -> SqlDataSet:
        return self._default_handler(
            node=node, function_to_convert_to_subquery=super().visit_metric_time_dimension_transform_node
        )

    @override
    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode) -> SqlDataSet:
        return self._default_handler(node=node, function_to_convert_to_subquery=super().visit_join_to_time_spine_node)

    @override
    def visit_min_max_node(self, node: MinMaxNode) -> SqlDataSet:
        return self._default_handler(node=node, function_to_convert_to_subquery=super().visit_min_max_node)

    @override
    def visit_add_generated_uuid_column_node(self, node: AddGeneratedUuidColumnNode) -> SqlDataSet:
        return self._default_handler(
            node=node, function_to_convert_to_subquery=super().visit_add_generated_uuid_column_node
        )

    @override
    def visit_join_conversion_events_node(self, node: JoinConversionEventsNode) -> SqlDataSet:
        return self._default_handler(
            node=node, function_to_convert_to_subquery=super().visit_join_conversion_events_node
        )

    @override
    def visit_join_to_custom_granularity_node(self, node: JoinToCustomGranularityNode) -> SqlDataSet:
        return self._default_handler(
            node=node, function_to_convert_to_subquery=super().visit_join_to_custom_granularity_node
        )
