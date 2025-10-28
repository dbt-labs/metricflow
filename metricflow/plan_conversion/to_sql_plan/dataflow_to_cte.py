from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Dict, FrozenSet, Sequence, Tuple, TypeVar

from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.dag.mf_dag import NodeId
from metricflow_semantics.instances import InstanceSet
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.nodes.add_generated_uuid import AddGeneratedUuidColumnNode
from metricflow.dataflow.nodes.aggregate_simple_metric_inputs import AggregateSimpleMetricInputsNode
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
from metricflow.dataflow.nodes.offset_base_grain_by_custom_grain import OffsetBaseGrainByCustomGrainNode
from metricflow.dataflow.nodes.offset_custom_granularity import OffsetCustomGranularityNode
from metricflow.dataflow.nodes.order_by_limit import OrderByLimitNode
from metricflow.dataflow.nodes.read_sql_source import ReadSqlSourceNode
from metricflow.dataflow.nodes.semi_additive_join import SemiAdditiveJoinNode
from metricflow.dataflow.nodes.where_filter import WhereConstraintNode
from metricflow.dataflow.nodes.window_reaggregation_node import WindowReaggregationNode
from metricflow.dataflow.nodes.write_to_data_table import WriteToResultDataTableNode
from metricflow.dataflow.nodes.write_to_table import WriteToResultTableNode
from metricflow.dataset.sql_dataset import SqlDataSet
from metricflow.plan_conversion.instance_set_transforms.select_columns import CreateSelectColumnsForInstances
from metricflow.plan_conversion.to_sql_plan.dataflow_to_subquery import DataflowNodeToSqlSubqueryVisitor
from metricflow.sql.sql_cte_node import SqlCteNode
from metricflow.sql.sql_plan import SqlSelectColumn
from metricflow.sql.sql_select_node import SqlSelectStatementNode
from metricflow.sql.sql_table_node import SqlTableNode

logger = logging.getLogger(__name__)


DataflowNodeT = TypeVar("DataflowNodeT", bound=DataflowPlanNode)


@dataclass(frozen=True)
class CteGenerationResult:
    """This stores parameters for creating a dataset from a CTE."""

    source_dataflow_plan_node_id: NodeId
    cte_node: SqlCteNode
    select_columns: Tuple[SqlSelectColumn, ...]
    instance_set: InstanceSet

    def get_sql_data_set(self) -> SqlDataSet:
        """Return a dataset that represents reading from the CTE."""
        cte_alias = self.cte_node.cte_alias
        return SqlDataSet(
            instance_set=self.instance_set,
            sql_select_node=SqlSelectStatementNode.create(
                description=f"Read From CTE For node_id={self.source_dataflow_plan_node_id}",
                select_columns=self.select_columns,
                from_source=SqlTableNode.create(SqlTable(schema_name=None, table_name=cte_alias)),
                from_source_alias=cte_alias,
            ),
        )


class DataflowNodeToSqlCteVisitor(DataflowNodeToSqlSubqueryVisitor):
    """Similar to `DataflowNodeToSqlSubqueryVisitor`, except that this converts specific nodes to CTEs.

    This is implemented as a subclass of `DataflowNodeToSqlSubqueryVisitor` so that by default, it has the same behavior
    but in cases where there are nodes that should be converted to CTEs, alternate methods can be used.

    The generated CTE nodes are collected instead of getting incorporated into the associated SQL query plan generated
    at each node so that the CTE nodes can be included at the top-level SELECT statement.

    # TODO: Move these visitors to separate files at the end of the stack.
    """

    def __init__(  # noqa: D107
        self,
        column_association_resolver: ColumnAssociationResolver,
        semantic_manifest_lookup: SemanticManifestLookup,
        nodes_to_convert_to_cte: FrozenSet[DataflowPlanNode],
        spec_output_order: Sequence[InstanceSpec],
    ) -> None:
        super().__init__(
            column_association_resolver=column_association_resolver,
            semantic_manifest_lookup=semantic_manifest_lookup,
            spec_output_order=spec_output_order,
        )
        self._nodes_to_convert_to_cte = nodes_to_convert_to_cte

        # If a given node is supposed to use a CTE, map the node to the result.
        self._node_to_cte_generation_result: Dict[DataflowPlanNode, CteGenerationResult] = {}

    def generated_cte_nodes(self) -> Sequence[SqlCteNode]:
        """Returns the CTE nodes that have been generated while traversing the dataflow plan."""
        return tuple(result.cte_node for result in self._node_to_cte_generation_result.values())

    def _default_handler(
        self,
        node: DataflowNodeT,
        node_to_select_subquery_function: Callable[[DataflowNodeT], SqlDataSet],
        use_spec_output_order: bool,
    ) -> SqlDataSet:
        """Default handler that is called for each node as the dataflow plan is traversed.

        Args:
            node: The current node in traversal.
            node_to_select_subquery_function: A function that converts the given node to a `SqlDataSet` where the
            SELECT statement source is a subquery. This should be a method in `DataflowNodeToSqlSubqueryVisitor` as this
            was the default behavior before CTEs were supported.

        Returns: The `SqlDataSet` that produces the data for the given node.
        """
        # For the given node, if a CTE was generated for it, return the associated data set.
        cte_generation_result = self._node_to_cte_generation_result.get(node)
        if cte_generation_result is not None:
            logger.debug(LazyFormat("Handling node via existing CTE", node=node))
            return cte_generation_result.get_sql_data_set()

        # If the given node is supposed to use a CTE, generate one for it. Otherwise, use the default subquery as the
        # source for the SELECT.
        select_from_subquery_dataset = node_to_select_subquery_function(node)
        if node not in self._nodes_to_convert_to_cte:
            logger.debug(LazyFormat("Handling node via subquery", node=node))
            return select_from_subquery_dataset
        logger.debug(LazyFormat("Handling node via new CTE", node=node))

        cte_alias = f"{node.node_id.id_str}_{StaticIdPrefix.CTE.value}"

        if cte_alias in set(node.cte_alias for node in self.generated_cte_nodes()):
            raise ValueError(
                f"{cte_alias=} is a duplicate of one that already exists. "
                f"This implies a bug that is generating a CTE for the same dataflow plan node multiple times."
            )

        cte_source = SqlCteNode.create(
            select_statement=select_from_subquery_dataset.sql_node,
            cte_alias=cte_alias,
        )

        cte_generation_result = CteGenerationResult(
            source_dataflow_plan_node_id=node.node_id,
            cte_node=cte_source,
            select_columns=CreateSelectColumnsForInstances(
                table_alias=cte_alias,
                column_resolver=self._column_association_resolver,
            )
            .transform(select_from_subquery_dataset.instance_set)
            .get_columns(self._spec_output_order if use_spec_output_order else ()),
            instance_set=select_from_subquery_dataset.instance_set,
        )
        self._node_to_cte_generation_result[node] = cte_generation_result
        return cte_generation_result.get_sql_data_set()

    @override
    def visit_source_node(self, node: ReadSqlSourceNode) -> SqlDataSet:
        return self._default_handler(
            node=node, node_to_select_subquery_function=super().visit_source_node, use_spec_output_order=False
        )

    @override
    def visit_join_on_entities_node(self, node: JoinOnEntitiesNode) -> SqlDataSet:
        return self._default_handler(
            node=node, node_to_select_subquery_function=super().visit_join_on_entities_node, use_spec_output_order=False
        )

    @override
    def visit_aggregate_simple_metric_inputs_node(self, node: AggregateSimpleMetricInputsNode) -> SqlDataSet:
        return self._default_handler(
            node=node,
            node_to_select_subquery_function=super().visit_aggregate_simple_metric_inputs_node,
            use_spec_output_order=False,
        )

    @override
    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> SqlDataSet:
        return self._default_handler(
            node=node, node_to_select_subquery_function=super().visit_compute_metrics_node, use_spec_output_order=False
        )

    @override
    def visit_window_reaggregation_node(self, node: WindowReaggregationNode) -> SqlDataSet:
        return self._default_handler(
            node=node,
            node_to_select_subquery_function=super().visit_window_reaggregation_node,
            use_spec_output_order=False,
        )

    @override
    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> SqlDataSet:
        return self._default_handler(
            node=node, node_to_select_subquery_function=super().visit_order_by_limit_node, use_spec_output_order=False
        )

    @override
    def visit_where_constraint_node(self, node: WhereConstraintNode) -> SqlDataSet:
        return self._default_handler(
            node=node, node_to_select_subquery_function=super().visit_where_constraint_node, use_spec_output_order=False
        )

    @override
    def visit_write_to_result_data_table_node(self, node: WriteToResultDataTableNode) -> SqlDataSet:
        return self._default_handler(
            node=node,
            node_to_select_subquery_function=super().visit_write_to_result_data_table_node,
            use_spec_output_order=True,
        )

    @override
    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> SqlDataSet:
        return self._default_handler(
            node=node,
            node_to_select_subquery_function=super().visit_write_to_result_table_node,
            use_spec_output_order=True,
        )

    @override
    def visit_filter_elements_node(self, node: FilterElementsNode) -> SqlDataSet:
        return self._default_handler(
            node=node, node_to_select_subquery_function=super().visit_filter_elements_node, use_spec_output_order=False
        )

    @override
    def visit_combine_aggregated_outputs_node(self, node: CombineAggregatedOutputsNode) -> SqlDataSet:
        return self._default_handler(
            node=node,
            node_to_select_subquery_function=super().visit_combine_aggregated_outputs_node,
            use_spec_output_order=False,
        )

    @override
    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode) -> SqlDataSet:
        return self._default_handler(
            node=node,
            node_to_select_subquery_function=super().visit_constrain_time_range_node,
            use_spec_output_order=False,
        )

    @override
    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode) -> SqlDataSet:
        return self._default_handler(
            node=node,
            node_to_select_subquery_function=super().visit_join_over_time_range_node,
            use_spec_output_order=False,
        )

    @override
    def visit_semi_additive_join_node(self, node: SemiAdditiveJoinNode) -> SqlDataSet:
        return self._default_handler(
            node=node,
            node_to_select_subquery_function=super().visit_semi_additive_join_node,
            use_spec_output_order=False,
        )

    @override
    def visit_metric_time_dimension_transform_node(self, node: MetricTimeDimensionTransformNode) -> SqlDataSet:
        return self._default_handler(
            node=node,
            node_to_select_subquery_function=super().visit_metric_time_dimension_transform_node,
            use_spec_output_order=False,
        )

    @override
    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode) -> SqlDataSet:
        return self._default_handler(
            node=node,
            node_to_select_subquery_function=super().visit_join_to_time_spine_node,
            use_spec_output_order=False,
        )

    @override
    def visit_min_max_node(self, node: MinMaxNode) -> SqlDataSet:
        return self._default_handler(
            node=node, node_to_select_subquery_function=super().visit_min_max_node, use_spec_output_order=False
        )

    @override
    def visit_add_generated_uuid_column_node(self, node: AddGeneratedUuidColumnNode) -> SqlDataSet:
        return self._default_handler(
            node=node,
            node_to_select_subquery_function=super().visit_add_generated_uuid_column_node,
            use_spec_output_order=False,
        )

    @override
    def visit_join_conversion_events_node(self, node: JoinConversionEventsNode) -> SqlDataSet:
        return self._default_handler(
            node=node,
            node_to_select_subquery_function=super().visit_join_conversion_events_node,
            use_spec_output_order=False,
        )

    @override
    def visit_join_to_custom_granularity_node(self, node: JoinToCustomGranularityNode) -> SqlDataSet:
        return self._default_handler(
            node=node,
            node_to_select_subquery_function=super().visit_join_to_custom_granularity_node,
            use_spec_output_order=False,
        )

    @override
    def visit_alias_specs_node(self, node: AliasSpecsNode) -> SqlDataSet:  # noqa: D102
        return self._default_handler(
            node=node, node_to_select_subquery_function=super().visit_alias_specs_node, use_spec_output_order=False
        )

    @override
    def visit_offset_base_grain_by_custom_grain_node(
        self, node: OffsetBaseGrainByCustomGrainNode
    ) -> SqlDataSet:  # noqa: D102
        return self._default_handler(
            node=node,
            node_to_select_subquery_function=super().visit_offset_base_grain_by_custom_grain_node,
            use_spec_output_order=False,
        )

    @override
    def visit_offset_custom_granularity_node(self, node: OffsetCustomGranularityNode) -> SqlDataSet:  # noqa: D102
        return self._default_handler(
            node=node,
            node_to_select_subquery_function=super().visit_offset_custom_granularity_node,
            use_spec_output_order=False,
        )
