from __future__ import annotations

import logging

from metricflow.sql.optimizer.sql_query_plan_optimizer import SqlQueryPlanOptimizer
from metricflow.sql.sql_plan import (
    SqlJoinDescription,
    SqlOrderByDescription,
    SqlQueryPlanNode,
    SqlQueryPlanNodeVisitor,
    SqlSelectColumn,
    SqlSelectQueryFromClauseNode,
    SqlSelectStatementNode,
    SqlTableFromClauseNode,
)

logger = logging.getLogger(__name__)


class SqlTableAliasSimplifierVisitor(SqlQueryPlanNodeVisitor[SqlQueryPlanNode]):
    """Visits the SQL query plan to see if table aliases can be omitted when rendering column references."""

    def visit_select_statement_node(self, node: SqlSelectStatementNode) -> SqlQueryPlanNode:  # noqa: D
        # If there is only a single parent, no table aliases are required since there's no ambiguity.
        should_simplify_table_aliases = len(node.parent_nodes) <= 1

        if should_simplify_table_aliases:
            return SqlSelectStatementNode(
                description=node.description,
                select_columns=tuple(
                    SqlSelectColumn(expr=x.expr.rewrite(should_render_table_alias=False), column_alias=x.column_alias)
                    for x in node.select_columns
                ),
                from_source=node.from_source.accept(self),
                from_source_alias=node.from_source_alias,
                joins_descs=(),
                group_bys=tuple(
                    SqlSelectColumn(expr=x.expr.rewrite(should_render_table_alias=False), column_alias=x.column_alias)
                    for x in node.group_bys
                ),
                order_bys=tuple(
                    SqlOrderByDescription(expr=x.expr.rewrite(should_render_table_alias=False), desc=x.desc)
                    for x in node.order_bys
                ),
                where=node.where.rewrite(should_render_table_alias=False) if node.where else None,
                limit=node.limit,
            )

        return SqlSelectStatementNode(
            description=node.description,
            select_columns=node.select_columns,
            from_source=node.from_source.accept(self),
            from_source_alias=node.from_source_alias,
            joins_descs=tuple(
                SqlJoinDescription(
                    right_source=x.right_source.accept(self),
                    right_source_alias=x.right_source_alias,
                    on_condition=x.on_condition,
                    join_type=x.join_type,
                )
                for x in node.join_descs
            ),
            group_bys=node.group_bys,
            order_bys=node.order_bys,
            where=node.where,
            limit=node.limit,
        )

    def visit_table_from_clause_node(self, node: SqlTableFromClauseNode) -> SqlQueryPlanNode:  # noqa: D
        return node

    def visit_query_from_clause_node(self, node: SqlSelectQueryFromClauseNode) -> SqlQueryPlanNode:  # noqa: D
        return node


class SqlTableAliasSimplifier(SqlQueryPlanOptimizer):
    """Simplify queries by eliminating table aliases when possible.

    e.g. from

    SELECT b.foo
    FROM (
      SELECT a.foo FROM bar a
    ) b

    to

    SELECT foo
    FROM (
      SELECT foo FROM bar a
    ) b
    """

    def optimize(self, node: SqlQueryPlanNode) -> SqlQueryPlanNode:  # noqa: D
        return node.accept(SqlTableAliasSimplifierVisitor())
