from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Dict, FrozenSet, Optional, Sequence, Set, Tuple, TypeVar

from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DagId, NodeId
from metricflow_semantics.instances import (
    InstanceSet,
)
from metricflow_semantics.mf_logging.formatting import indent
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.time.time_spine_source import TimeSpineSource
from typing_extensions import override

from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
)
from metricflow.dataflow.dataflow_plan_analyzer import DataflowPlanAnalyzer
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
from metricflow.dataflow.nodes.offset_by_custom_granularity import OffsetByCustomGranularityNode
from metricflow.dataflow.nodes.order_by_limit import OrderByLimitNode
from metricflow.dataflow.nodes.read_sql_source import ReadSqlSourceNode
from metricflow.dataflow.nodes.semi_additive_join import SemiAdditiveJoinNode
from metricflow.dataflow.nodes.where_filter import WhereConstraintNode
from metricflow.dataflow.nodes.window_reaggregation_node import WindowReaggregationNode
from metricflow.dataflow.nodes.write_to_data_table import WriteToResultDataTableNode
from metricflow.dataflow.nodes.write_to_table import WriteToResultTableNode
from metricflow.dataset.sql_dataset import SqlDataSet
from metricflow.plan_conversion.convert_to_sql_plan import ConvertToSqlPlanResult
from metricflow.plan_conversion.instance_converters import (
    CreateSelectColumnsForInstances,
)
from metricflow.plan_conversion.to_sql_plan.dataflow_to_subquery import DataflowNodeToSqlSubqueryVisitor
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.optimizer.optimization_levels import (
    SqlGenerationOptionSet,
    SqlOptimizationLevel,
)
from metricflow.sql.optimizer.sql_query_plan_optimizer import SqlPlanOptimizer
from metricflow.sql.sql_plan import (
    SqlCteNode,
    SqlPlan,
    SqlPlanNode,
    SqlSelectColumn,
    SqlSelectStatementNode,
    SqlTableNode,
)

logger = logging.getLogger(__name__)


class DataflowToSqlPlanConverter:
    """Generates an SQL query plan from a node in the metric dataflow plan."""

    def __init__(
        self,
        column_association_resolver: ColumnAssociationResolver,
        semantic_manifest_lookup: SemanticManifestLookup,
    ) -> None:
        """Constructor.

        Args:
            column_association_resolver: controls how columns for instances are generated and used between nested
            queries.
            semantic_manifest_lookup: Self-explanatory.
        """
        self._column_association_resolver = column_association_resolver
        self._semantic_manifest_lookup = semantic_manifest_lookup
        self._metric_lookup = semantic_manifest_lookup.metric_lookup
        self._semantic_model_lookup = semantic_manifest_lookup.semantic_model_lookup
        self._time_spine_sources = TimeSpineSource.build_standard_time_spine_sources(
            semantic_manifest_lookup.semantic_manifest
        )
        self._custom_granularity_time_spine_sources = TimeSpineSource.build_custom_time_spine_sources(
            tuple(self._time_spine_sources.values())
        )

    @property
    def column_association_resolver(self) -> ColumnAssociationResolver:  # noqa: D102
        return self._column_association_resolver

    def convert_to_sql_plan(
        self,
        sql_engine_type: SqlEngine,
        dataflow_plan_node: DataflowPlanNode,
        optimization_level: SqlOptimizationLevel = SqlOptimizationLevel.default_level(),
        sql_query_plan_id: Optional[DagId] = None,
    ) -> ConvertToSqlPlanResult:
        """Create an SQL query plan that represents the computation up to the given dataflow plan node."""
        # In case there are bugs that raise exceptions at higher optimization levels, retry generation at a lower
        # optimization level. Generally skip O0 (unless requested) as that level does not include the column pruner.
        # Without that, the generated SQL can be enormous.
        optimization_levels_to_attempt: Sequence[SqlOptimizationLevel] = sorted(
            # Union handles case if O0 was specifically requested.
            set(
                possible_level
                for possible_level in SqlOptimizationLevel
                if SqlOptimizationLevel.O1 <= possible_level <= optimization_level
            ).union({optimization_level}),
            reverse=True,
        )
        retried_at_lower_optimization_level = False
        logger.debug(
            LazyFormat(
                "Attempting to convert to a SQL plan with optimization levels:",
                optimization_levels_to_attempt=optimization_levels_to_attempt,
            )
        )
        for attempted_optimization_level in optimization_levels_to_attempt:
            try:
                # TODO: Make this a more generally accessible attribute instead of checking against the
                # BigQuery-ness of the engine
                use_column_alias_in_group_by = sql_engine_type is SqlEngine.BIGQUERY

                option_set = SqlGenerationOptionSet.options_for_level(
                    attempted_optimization_level, use_column_alias_in_group_by=use_column_alias_in_group_by
                )

                logger.info(
                    LazyFormat(
                        "Using option set for SQL generation:",
                        optimization_level=optimization_level,
                        option_set=option_set,
                    )
                )

                nodes_to_convert_to_cte: FrozenSet[DataflowPlanNode] = frozenset()
                if option_set.allow_cte:
                    nodes_to_convert_to_cte = self._get_nodes_to_convert_to_cte(dataflow_plan_node)

                result = self.convert_using_specifics(
                    dataflow_plan_node=dataflow_plan_node,
                    sql_query_plan_id=sql_query_plan_id,
                    nodes_to_convert_to_cte=nodes_to_convert_to_cte,
                    optimizers=option_set.optimizers,
                )

                if retried_at_lower_optimization_level:
                    logger.error(
                        LazyFormat(
                            "Successfully generated the SQL plan using an optimization level lower than the"
                            " requested one. A lower one was used due to an exception using the requested one. Please "
                            "investigate the cause for the exception.",
                            requested_optimization_level=optimization_level,
                            successful_optimization_level=attempted_optimization_level,
                        )
                    )

                return result

            except Exception as e:
                if optimization_level is optimization_levels_to_attempt[-1]:
                    logger.error(
                        "Exhausted attempts to generate the SQL without exceptions."
                        " Propagating the most recent exception."
                    )
                    raise e
                retried_at_lower_optimization_level = True
                logger.exception(
                    LazyFormat(
                        "Got an exception while generating the SQL plan. This indicates a bug that should be"
                        " investigated, but retrying at a different optimization level to potentially avoid a"
                        " user-facing error.",
                        attempted_optimization_level=optimization_level,
                    )
                )

        raise RuntimeError("Should have returned a result or raised an exception in the loop.")

    def convert_using_specifics(
        self,
        dataflow_plan_node: DataflowPlanNode,
        sql_query_plan_id: Optional[DagId],
        nodes_to_convert_to_cte: FrozenSet[DataflowPlanNode],
        optimizers: Sequence[SqlPlanOptimizer],
    ) -> ConvertToSqlPlanResult:
        """Helper method to convert using specific options. Main use case are tests."""
        logger.debug(
            LazyFormat("Converting to SQL", nodes_to_convert_to_cte=[node.node_id for node in nodes_to_convert_to_cte])
        )

        if len(nodes_to_convert_to_cte) == 0:
            # Avoid `DataflowNodeToSqlCteVisitor` code path for better isolation during rollout.
            # Later this branch can be removed as `DataflowNodeToSqlCteVisitor` should handle an empty
            # `dataflow_nodes_to_convert_to_cte`.
            to_sql_subquery_visitor = DataflowNodeToSqlSubqueryVisitor(
                column_association_resolver=self.column_association_resolver,
                semantic_manifest_lookup=self._semantic_manifest_lookup,
            )
            data_set = dataflow_plan_node.accept(to_sql_subquery_visitor)
        else:
            to_sql_cte_visitor = DataflowNodeToSqlCteVisitor(
                column_association_resolver=self.column_association_resolver,
                semantic_manifest_lookup=self._semantic_manifest_lookup,
                nodes_to_convert_to_cte=nodes_to_convert_to_cte,
            )
            data_set = dataflow_plan_node.accept(to_sql_cte_visitor)
            select_statement = data_set.checked_sql_select_node
            data_set = SqlDataSet(
                instance_set=data_set.instance_set,
                sql_select_node=SqlSelectStatementNode.create(
                    description=select_statement.description,
                    select_columns=select_statement.select_columns,
                    from_source=select_statement.from_source,
                    from_source_alias=select_statement.from_source_alias,
                    cte_sources=tuple(to_sql_cte_visitor.generated_cte_nodes()),
                    join_descs=select_statement.join_descs,
                    group_bys=select_statement.group_bys,
                    order_bys=select_statement.order_bys,
                    where=select_statement.where,
                    limit=select_statement.limit,
                    distinct=select_statement.distinct,
                ),
            )

        sql_node: SqlPlanNode = data_set.sql_node

        for optimizer in optimizers:
            logger.debug(LazyFormat(lambda: f"Applying optimizer: {optimizer.__class__.__name__}"))
            sql_node = optimizer.optimize(sql_node)
            logger.debug(
                LazyFormat(
                    lambda: f"After applying optimizer {optimizer.__class__.__name__}, the SQL query plan is:\n"
                    f"{indent(sql_node.structure_text())}"
                )
            )

        return ConvertToSqlPlanResult(
            instance_set=data_set.instance_set,
            sql_plan=SqlPlan(render_node=sql_node, plan_id=sql_query_plan_id),
        )

    def _get_nodes_to_convert_to_cte(
        self,
        dataflow_plan_node: DataflowPlanNode,
    ) -> FrozenSet[DataflowPlanNode]:
        """Handles logic for selecting which nodes to convert to CTEs based on the request."""
        dataflow_plan = dataflow_plan_node.as_plan()
        nodes_to_convert_to_cte: Set[DataflowPlanNode] = set(DataflowPlanAnalyzer.find_common_branches(dataflow_plan))
        # Additional nodes will be added later.

        return frozenset(nodes_to_convert_to_cte)


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
    ) -> None:
        super().__init__(
            column_association_resolver=column_association_resolver, semantic_manifest_lookup=semantic_manifest_lookup
        )
        self._nodes_to_convert_to_cte = nodes_to_convert_to_cte

        # If a given node is supposed to use a CTE, map the node to the result.
        self._node_to_cte_generation_result: Dict[DataflowPlanNode, CteGenerationResult] = {}

    def generated_cte_nodes(self) -> Sequence[SqlCteNode]:
        """Returns the CTE nodes that have been generated while traversing the dataflow plan."""
        return tuple(result.cte_node for result in self._node_to_cte_generation_result.values())

    def _default_handler(
        self, node: DataflowNodeT, node_to_select_subquery_function: Callable[[DataflowNodeT], SqlDataSet]
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
            .as_tuple(),
            instance_set=select_from_subquery_dataset.instance_set,
        )
        self._node_to_cte_generation_result[node] = cte_generation_result
        return cte_generation_result.get_sql_data_set()

    @override
    def visit_source_node(self, node: ReadSqlSourceNode) -> SqlDataSet:
        return self._default_handler(node=node, node_to_select_subquery_function=super().visit_source_node)

    @override
    def visit_join_on_entities_node(self, node: JoinOnEntitiesNode) -> SqlDataSet:
        return self._default_handler(node=node, node_to_select_subquery_function=super().visit_join_on_entities_node)

    @override
    def visit_aggregate_measures_node(self, node: AggregateMeasuresNode) -> SqlDataSet:
        return self._default_handler(node=node, node_to_select_subquery_function=super().visit_aggregate_measures_node)

    @override
    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> SqlDataSet:
        return self._default_handler(node=node, node_to_select_subquery_function=super().visit_compute_metrics_node)

    @override
    def visit_window_reaggregation_node(self, node: WindowReaggregationNode) -> SqlDataSet:
        return self._default_handler(
            node=node, node_to_select_subquery_function=super().visit_window_reaggregation_node
        )

    @override
    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> SqlDataSet:
        return self._default_handler(node=node, node_to_select_subquery_function=super().visit_order_by_limit_node)

    @override
    def visit_where_constraint_node(self, node: WhereConstraintNode) -> SqlDataSet:
        return self._default_handler(node=node, node_to_select_subquery_function=super().visit_where_constraint_node)

    @override
    def visit_write_to_result_data_table_node(self, node: WriteToResultDataTableNode) -> SqlDataSet:
        return self._default_handler(
            node=node, node_to_select_subquery_function=super().visit_write_to_result_data_table_node
        )

    @override
    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> SqlDataSet:
        return self._default_handler(
            node=node, node_to_select_subquery_function=super().visit_write_to_result_table_node
        )

    @override
    def visit_filter_elements_node(self, node: FilterElementsNode) -> SqlDataSet:
        return self._default_handler(node=node, node_to_select_subquery_function=super().visit_filter_elements_node)

    @override
    def visit_combine_aggregated_outputs_node(self, node: CombineAggregatedOutputsNode) -> SqlDataSet:
        return self._default_handler(
            node=node, node_to_select_subquery_function=super().visit_combine_aggregated_outputs_node
        )

    @override
    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode) -> SqlDataSet:
        return self._default_handler(
            node=node, node_to_select_subquery_function=super().visit_constrain_time_range_node
        )

    @override
    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode) -> SqlDataSet:
        return self._default_handler(
            node=node, node_to_select_subquery_function=super().visit_join_over_time_range_node
        )

    @override
    def visit_semi_additive_join_node(self, node: SemiAdditiveJoinNode) -> SqlDataSet:
        return self._default_handler(node=node, node_to_select_subquery_function=super().visit_semi_additive_join_node)

    @override
    def visit_metric_time_dimension_transform_node(self, node: MetricTimeDimensionTransformNode) -> SqlDataSet:
        return self._default_handler(
            node=node, node_to_select_subquery_function=super().visit_metric_time_dimension_transform_node
        )

    @override
    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode) -> SqlDataSet:
        return self._default_handler(node=node, node_to_select_subquery_function=super().visit_join_to_time_spine_node)

    @override
    def visit_min_max_node(self, node: MinMaxNode) -> SqlDataSet:
        return self._default_handler(node=node, node_to_select_subquery_function=super().visit_min_max_node)

    @override
    def visit_add_generated_uuid_column_node(self, node: AddGeneratedUuidColumnNode) -> SqlDataSet:
        return self._default_handler(
            node=node, node_to_select_subquery_function=super().visit_add_generated_uuid_column_node
        )

    @override
    def visit_join_conversion_events_node(self, node: JoinConversionEventsNode) -> SqlDataSet:
        return self._default_handler(
            node=node, node_to_select_subquery_function=super().visit_join_conversion_events_node
        )

    @override
    def visit_join_to_custom_granularity_node(self, node: JoinToCustomGranularityNode) -> SqlDataSet:
        return self._default_handler(
            node=node, node_to_select_subquery_function=super().visit_join_to_custom_granularity_node
        )

    @override
    def visit_alias_specs_node(self, node: AliasSpecsNode) -> SqlDataSet:  # noqa: D102
        return self._default_handler(node=node, node_to_select_subquery_function=super().visit_alias_specs_node)

    @override
    def visit_offset_by_custom_granularity_node(self, node: OffsetByCustomGranularityNode) -> SqlDataSet:  # noqa: D102
        return self._default_handler(
            node=node, node_to_select_subquery_function=super().visit_offset_by_custom_granularity_node
        )


DataflowNodeT = TypeVar("DataflowNodeT", bound=DataflowPlanNode)
