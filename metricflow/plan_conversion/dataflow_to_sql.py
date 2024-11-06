from __future__ import annotations

import logging
from typing import Optional

from metricflow_semantics.dag.mf_dag import DagId
from metricflow_semantics.mf_logging.formatting import indent
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.specs.column_assoc import (
    ColumnAssociationResolver,
)
from metricflow_semantics.time.time_spine_source import TimeSpineSource

from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
)
from metricflow.plan_conversion.convert_to_sql_plan import ConvertToSqlPlanResult
from metricflow.plan_conversion.dataflow_to_sql_subquery import DataflowNodeToSqlSubqueryVisitor
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.optimizer.optimization_levels import (
    SqlQueryOptimizationLevel,
    SqlQueryOptimizerConfiguration,
)
from metricflow.sql.sql_plan import (
    SqlQueryPlan,
    SqlQueryPlanNode,
)

logger = logging.getLogger(__name__)


class DataflowToSqlQueryPlanConverter:
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

    def convert_to_sql_query_plan(
        self,
        sql_engine_type: SqlEngine,
        dataflow_plan_node: DataflowPlanNode,
        optimization_level: SqlQueryOptimizationLevel = SqlQueryOptimizationLevel.O4,
        sql_query_plan_id: Optional[DagId] = None,
    ) -> ConvertToSqlPlanResult:
        """Create an SQL query plan that represents the computation up to the given dataflow plan node."""
        to_sql_visitor = DataflowNodeToSqlSubqueryVisitor(
            column_association_resolver=self.column_association_resolver,
            semantic_manifest_lookup=self._semantic_manifest_lookup,
        )
        data_set = dataflow_plan_node.accept(to_sql_visitor)

        sql_node: SqlQueryPlanNode = data_set.sql_node
        # TODO: Make this a more generally accessible attribute instead of checking against the
        # BigQuery-ness of the engine
        use_column_alias_in_group_by = sql_engine_type is SqlEngine.BIGQUERY

        for optimizer in SqlQueryOptimizerConfiguration.optimizers_for_level(
            optimization_level, use_column_alias_in_group_by=use_column_alias_in_group_by
        ):
            logger.debug(LazyFormat(lambda: f"Applying optimizer: {optimizer.__class__.__name__}"))
            sql_node = optimizer.optimize(sql_node)
            logger.debug(
                LazyFormat(
                    lambda: f"After applying {optimizer.__class__.__name__}, the SQL query plan is:\n"
                    f"{indent(sql_node.structure_text())}"
                )
            )

        return ConvertToSqlPlanResult(
            instance_set=data_set.instance_set,
            sql_plan=SqlQueryPlan(render_node=sql_node, plan_id=sql_query_plan_id),
        )
