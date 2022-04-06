import logging
from typing import List, Optional

from metricflow.sql.optimizer.sql_query_plan_optimizer import SqlQueryPlanOptimizer
from metricflow.sql.sql_exprs import SqlColumnReferenceExpression, SqlColumnReference
from metricflow.sql.sql_plan import (
    SqlQueryPlanNode,
    SqlQueryPlanNodeVisitor,
    SqlSelectQueryFromClauseNode,
    SqlTableFromClauseNode,
    SqlSelectStatementNode,
    SqlOrderByDescription,
    SqlJoinDescription,
)

logger = logging.getLogger(__name__)


class SqlSubQueryReducerVisitor(SqlQueryPlanNodeVisitor[SqlQueryPlanNode]):
    """Visits the SQL query plan to simplify sub-queries. On each visit, return a simplfied node"""

    def _reduce_parents(
        self,
        node: SqlSelectStatementNode,
    ) -> SqlSelectStatementNode:
        """Apply the reducing operation to the parent select statements."""
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

    def _reduce_is_possible(self, node: SqlSelectStatementNode) -> bool:  # noqa: D
        """Returns true if the given node can be reduced with the parent node.

        Reducing this node means eliminating the SELECT of this node and merging it with the parent SELECT. This
        checks for obvious cases where reducing can't happen, but there are cases where reducing is possible, but this
        returns false for ease of reasoning.
        """

        # If this node has multiple parents (i.e. a join), then this can't be collapsed.
        is_join = len(node.join_descs) > 0
        has_multiple_parent_nodes = len(node.parent_nodes) > 1
        if has_multiple_parent_nodes or is_join:
            return False

        assert len(node.parent_nodes) == 1
        parent_node = node.parent_nodes[0]

        # If the parent node is not a select statement, then this can't be collapsed. e.g. with a table as a parent like
        # SELECT foo FROM bar
        if not parent_node.as_select_node:
            return False
        parent_select_node = parent_node.as_select_node

        # More conditions where we don't want to collapse. It's not impossible with these cases, but not reducing in
        # these cases for simplicity.

        # Reducing a where is tricky as it requires the expressions to be re-written.
        if node.where:
            return False

        # Group bys are hard to reduce.
        if len(node.group_bys) > 0:
            return False

        # Similar with order bys.
        if len(node.order_bys) > 0 and len(parent_select_node.order_bys) > 0:
            return False

        # Can't reduce if the sub-query selects more columns. This generally won't be hit as long as the column pruner
        # runs before this optimization.
        if len(parent_select_node.select_columns) > len(node.select_columns):
            return False

        # For now, simplify only in cases where the select expressions directly reference columns in the parent query
        # and the alias is the same as the column reference. This means that the column references don't have to be
        # re-written when this is collapsed into the parent subquery.
        for select_column in node.select_columns:
            if not select_column.expr.as_column_reference_expression:
                return False
            column_reference_expression = select_column.expr.as_column_reference_expression
            assert column_reference_expression
            if column_reference_expression.col_ref.column_name != select_column.column_alias:
                return False

        # Same thing with order by
        for order_by in node.order_bys:
            if not order_by.expr.as_column_reference_expression:
                return False

        return True

    @staticmethod
    def _find_matching_table_alias(node: SqlSelectStatementNode, column_alias: str) -> Optional[str]:
        for select_column in node.select_columns:
            if select_column.column_alias == column_alias:
                column_reference_expr = select_column.expr.as_column_reference_expression
                if column_reference_expr:
                    return column_reference_expr.col_ref.table_alias
        return None

    def visit_select_statement_node(self, node: SqlSelectStatementNode) -> SqlQueryPlanNode:  # noqa: D
        node_with_reduced_parents = self._reduce_parents(node)

        if not self._reduce_is_possible(node_with_reduced_parents):
            return node_with_reduced_parents

        assert len(node_with_reduced_parents.parent_nodes) == 1
        parent_node = node_with_reduced_parents.parent_nodes[0]
        parent_select_node = parent_node.as_select_node
        assert parent_select_node

        # At this point, the query should look similar to
        #
        #     SELECT b.foo, b.baz
        #     FROM (
        #       SELECT a.foo, a.baz FROM bar a
        #       LIMIT 10
        #     ) b
        #     ORDER by b.baz
        #     LIMIT 1

        # The order by in the parent doesn't matter since the order by in this node will "overwrite" the order in the
        # parent as long as the parent has no limits.
        new_order_by: List[SqlOrderByDescription] = []
        if node_with_reduced_parents.order_bys:
            for order_by_item in node_with_reduced_parents.order_bys:
                order_by_item_expr = order_by_item.expr.as_column_reference_expression
                assert order_by_item_expr

                # Re-write the order by as a column expression that references the table alias
                table_alias_in_parent = SqlSubQueryReducerVisitor._find_matching_table_alias(
                    parent_select_node, order_by_item_expr.col_ref.column_name
                )
                if not table_alias_in_parent:
                    return node_with_reduced_parents
                new_order_by.append(
                    SqlOrderByDescription(
                        expr=SqlColumnReferenceExpression(
                            SqlColumnReference(
                                table_alias=table_alias_in_parent,
                                column_name=order_by_item_expr.col_ref.column_name,
                            )
                        ),
                        desc=order_by_item.desc,
                    )
                )

        # The limit should be the min of this SELECT limit and the parent SELECT limit.
        new_limit: Optional[int] = node_with_reduced_parents.limit
        if new_limit is None:
            new_limit = parent_select_node.limit
        elif parent_select_node.limit is not None:
            new_limit = min(new_limit, parent_select_node.limit)

        return SqlSelectStatementNode(
            description="\n".join([parent_select_node.description, node_with_reduced_parents.description]),
            select_columns=parent_select_node.select_columns,
            from_source=parent_select_node.from_source,
            from_source_alias=parent_select_node.from_source_alias,
            joins_descs=parent_select_node.join_descs,
            group_bys=parent_select_node.group_bys,
            order_bys=tuple(new_order_by),
            where=parent_select_node.where,
            limit=new_limit,
        )

    def visit_table_from_clause_node(self, node: SqlTableFromClauseNode) -> SqlQueryPlanNode:  # noqa: D
        return node

    def visit_query_from_clause_node(self, node: SqlSelectQueryFromClauseNode) -> SqlQueryPlanNode:  # noqa: D
        return node


class SqlSubQueryReducer(SqlQueryPlanOptimizer):
    """Simplify queries by eliminating sub-queries when possible.

    e.g. from

    SELECT b.foo
    FROM (
      SELECT a.foo FROM bar a
    ) b

    to

    SELECT a.foo FROM bar a
    """

    def optimize(self, node: SqlQueryPlanNode) -> SqlQueryPlanNode:  # noqa: D
        return node.accept(SqlSubQueryReducerVisitor())
