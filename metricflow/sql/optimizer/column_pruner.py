from __future__ import annotations

import logging

from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

from metricflow.sql.optimizer.required_column_aliases import SqlMapRequiredColumnAliasesVisitor
from metricflow.sql.optimizer.sql_query_plan_optimizer import SqlQueryPlanOptimizer
from metricflow.sql.optimizer.tag_column_aliases import NodeToColumnAliasMapping
from metricflow.sql.sql_plan import (
    SqlCreateTableAsNode,
    SqlCteNode,
    SqlQueryPlanNode,
    SqlQueryPlanNodeVisitor,
    SqlSelectQueryFromClauseNode,
    SqlSelectStatementNode,
    SqlTableNode,
)

logger = logging.getLogger(__name__)


class SqlColumnPrunerVisitor(SqlQueryPlanNodeVisitor[SqlQueryPlanNode]):
    """Removes unnecessary columns from SELECT statements in the SQL query plan.

    This requires a set of tagged column aliases that should be kept for each SQL node.
    """

    def __init__(
        self,
        required_alias_mapping: NodeToColumnAliasMapping,
    ) -> None:
        """Constructor.

        Args:
            required_alias_mapping: Describes columns aliases that should be kept / not pruned for each node.
        """
        self._required_alias_mapping = required_alias_mapping

    def visit_select_statement_node(self, node: SqlSelectStatementNode) -> SqlQueryPlanNode:  # noqa: D102
        # Remove columns that are not needed from this SELECT statement because the parent SELECT statement doesn't
        # need them. However, keep columns that are in group bys because that changes the meaning of the query.
        # Similarly, if this node is a distinct select node, keep all columns as it may return a different result set.
        required_column_aliases = self._required_alias_mapping.get_aliases(node)
        if required_column_aliases is None:
            logger.error(
                f"Did not find {node.node_id=} in the required alias mapping. Returning the non-pruned version "
                f"as it should be valid SQL, but this is a bug and should be investigated."
            )
            return node

        if len(required_column_aliases) == 0:
            logger.error(
                f"Got no required column aliases for {node}. Returning the non-pruned version as it should be valid "
                f"SQL, but this is a bug and should be investigated."
            )
            return node

        pruned_select_columns = tuple(
            select_column
            for select_column in node.select_columns
            if select_column.column_alias in required_column_aliases
        )

        return SqlSelectStatementNode.create(
            description=node.description,
            select_columns=pruned_select_columns,
            from_source=node.from_source.accept(self),
            from_source_alias=node.from_source_alias,
            cte_sources=tuple(
                cte_source.with_new_select(cte_source.select_statement.accept(self)) for cte_source in node.cte_sources
            ),
            join_descs=tuple(
                join_desc.with_right_source(join_desc.right_source.accept(self)) for join_desc in node.join_descs
            ),
            group_bys=node.group_bys,
            order_bys=node.order_bys,
            where=node.where,
            limit=node.limit,
            distinct=node.distinct,
        )

    def visit_table_node(self, node: SqlTableNode) -> SqlQueryPlanNode:
        """There are no SELECT columns in this node, so pruning cannot apply."""
        return node

    def visit_query_from_clause_node(self, node: SqlSelectQueryFromClauseNode) -> SqlQueryPlanNode:
        """Pruning cannot be done here since this is an arbitrary user-provided SQL query."""
        return node

    def visit_create_table_as_node(self, node: SqlCreateTableAsNode) -> SqlQueryPlanNode:  # noqa: D102
        return SqlCreateTableAsNode.create(
            sql_table=node.sql_table,
            parent_node=node.parent_node.accept(self),
        )

    @override
    def visit_cte_node(self, node: SqlCteNode) -> SqlQueryPlanNode:
        return node.with_new_select(node.select_statement.accept(self))


class SqlColumnPrunerOptimizer(SqlQueryPlanOptimizer):
    """Removes unnecessary columns in the SELECT statements."""

    def optimize(self, node: SqlQueryPlanNode) -> SqlQueryPlanNode:  # noqa: D102
        # ALl columns in the nearest SELECT node need to be kept as otherwise, the meaning of the query changes.
        required_select_columns = node.nearest_select_columns({})

        # Can't prune without knowing the structure of the query.
        if required_select_columns is None:
            logger.error(
                LazyFormat(
                    "The columns required at this node can't be determined, so skipping column pruning",
                    node=node.structure_text(),
                    required_select_columns=required_select_columns,
                )
            )
            return node

        map_required_column_aliases_visitor = SqlMapRequiredColumnAliasesVisitor(
            start_node=node,
            required_column_aliases_in_start_node=frozenset(
                [select_column.column_alias for select_column in required_select_columns]
            ),
        )
        node.accept(map_required_column_aliases_visitor)

        # Re-write the query, removing unnecessary columns in the SELECT statements.
        pruning_visitor = SqlColumnPrunerVisitor(map_required_column_aliases_visitor.required_column_alias_mapping)
        return node.accept(pruning_visitor)
