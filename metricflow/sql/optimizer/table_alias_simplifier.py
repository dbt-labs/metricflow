from __future__ import annotations

import logging

from typing_extensions import override

from metricflow.sql.optimizer.sql_query_plan_optimizer import SqlPlanOptimizer
from metricflow.sql.sql_ctas_node import SqlCreateTableAsNode
from metricflow.sql.sql_cte_node import SqlCteNode
from metricflow.sql.sql_plan import (
    SqlPlanNode,
    SqlPlanNodeVisitor,
    SqlSelectColumn,
)
from metricflow.sql.sql_select_node import SqlJoinDescription, SqlOrderByDescription, SqlSelectStatementNode
from metricflow.sql.sql_select_text_node import SqlSelectTextNode
from metricflow.sql.sql_table_node import SqlTableNode

logger = logging.getLogger(__name__)


class SqlTableAliasSimplifierVisitor(SqlPlanNodeVisitor[SqlPlanNode]):
    """Visits the SQL query plan to see if table aliases can be omitted when rendering column references."""

    @override
    def visit_cte_node(self, node: SqlCteNode) -> SqlPlanNode:
        return node.with_new_select(node.select_statement.accept(self))

    def visit_select_statement_node(self, node: SqlSelectStatementNode) -> SqlPlanNode:  # noqa: D102
        # If there is only a single source in the SELECT, no table aliases are required since there's no ambiguity.
        should_simplify_table_aliases = len(node.join_descs) == 0

        if should_simplify_table_aliases:
            return SqlSelectStatementNode.create(
                description=node.description,
                select_columns=tuple(
                    SqlSelectColumn(expr=x.expr.rewrite(should_render_table_alias=False), column_alias=x.column_alias)
                    for x in node.select_columns
                ),
                from_source=node.from_source.accept(self),
                from_source_alias=node.from_source_alias,
                cte_sources=tuple(
                    cte_source.with_new_select(cte_source.select_statement.accept(self))
                    for cte_source in node.cte_sources
                ),
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
                distinct=node.distinct,
            )

        return SqlSelectStatementNode.create(
            description=node.description,
            select_columns=node.select_columns,
            from_source=node.from_source.accept(self),
            from_source_alias=node.from_source_alias,
            cte_sources=tuple(
                cte_source.with_new_select(cte_source.select_statement.accept(self)) for cte_source in node.cte_sources
            ),
            join_descs=tuple(
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
            distinct=node.distinct,
        )

    def visit_table_node(self, node: SqlTableNode) -> SqlPlanNode:  # noqa: D102
        return node

    def visit_query_from_clause_node(self, node: SqlSelectTextNode) -> SqlPlanNode:  # noqa: D102
        return node

    def visit_create_table_as_node(self, node: SqlCreateTableAsNode) -> SqlPlanNode:  # noqa: D102
        return SqlCreateTableAsNode.create(
            sql_table=node.sql_table,
            parent_node=node.parent_node.accept(self),
        )


class SqlTableAliasSimplifier(SqlPlanOptimizer):
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

    def optimize(self, node: SqlPlanNode) -> SqlPlanNode:  # noqa: D102
        return node.accept(SqlTableAliasSimplifierVisitor())
