from __future__ import annotations

import logging
from typing import FrozenSet, Optional, Sequence, Set

from metricflow_semantics.dag.mf_dag import DagId
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.time.time_spine_source import TimeSpineSource
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.string_helpers import mf_indent

from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
)
from metricflow.dataflow.dataflow_plan_analyzer import DataflowPlanAnalyzer
from metricflow.dataset.sql_dataset import SqlDataSet
from metricflow.plan_conversion.convert_to_sql_plan import ConvertToSqlPlanResult
from metricflow.plan_conversion.to_sql_plan.dataflow_to_cte import DataflowNodeToSqlCteVisitor
from metricflow.plan_conversion.to_sql_plan.dataflow_to_subquery import DataflowNodeToSqlSubqueryVisitor
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.optimizer.optimization_levels import (
    SqlGenerationOptionSet,
    SqlOptimizationLevel,
)
from metricflow.sql.optimizer.sql_query_plan_optimizer import SqlPlanOptimizer
from metricflow.sql.sql_plan import (
    SqlPlan,
    SqlPlanNode,
)
from metricflow.sql.sql_select_node import SqlSelectStatementNode

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
        spec_output_order: Sequence[InstanceSpec] = (),
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
                    spec_output_order=spec_output_order,
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
                if attempted_optimization_level is optimization_levels_to_attempt[-1]:
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
        spec_output_order: Sequence[InstanceSpec],
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
                spec_output_order=spec_output_order,
            )
            data_set = to_sql_subquery_visitor.get_output_data_set(dataflow_plan_node)
        else:
            to_sql_cte_visitor = DataflowNodeToSqlCteVisitor(
                column_association_resolver=self.column_association_resolver,
                semantic_manifest_lookup=self._semantic_manifest_lookup,
                nodes_to_convert_to_cte=nodes_to_convert_to_cte,
                spec_output_order=spec_output_order,
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
                    f"{mf_indent(sql_node.structure_text())}"
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
