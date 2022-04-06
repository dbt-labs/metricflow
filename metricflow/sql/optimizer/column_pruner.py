from __future__ import annotations

import logging
from collections import defaultdict
from typing import Tuple, List, Set, Dict

from metricflow.sql.optimizer.sql_query_plan_optimizer import SqlQueryPlanOptimizer
from metricflow.sql.sql_exprs import (
    SqlExpressionTreeLineage,
)
from metricflow.sql.sql_plan import (
    SqlQueryPlanNode,
    SqlQueryPlanNodeVisitor,
    SqlTableFromClauseNode,
    SqlSelectStatementNode,
    SqlJoinDescription,
    SqlSelectQueryFromClauseNode,
    SqlSelectColumn,
)

logger = logging.getLogger(__name__)


class SqlColumnPrunerVisitor(SqlQueryPlanNodeVisitor[SqlQueryPlanNode]):
    """Removes unnecessary columns from SELECT statements in the SQL query plan.

    As the visitor traverses up to the parents, it pushes the list of required columns and rewrites the parent nodes.
    """

    def __init__(
        self,
        required_column_aliases: Set[str],
    ) -> None:
        """Constructor.

        Args:
            required_column_aliases: the columns aliases that should not be pruned from the SELECT statements that this
            visits.
        """

        self._required_column_aliases = required_column_aliases

    def _search_for_expressions(
        self, select_node: SqlSelectStatementNode, pruned_select_columns: Tuple[SqlSelectColumn, ...]
    ) -> SqlExpressionTreeLineage:
        """Returns the expressions used in the immediate select statement.

        i.e. this does not return expressions used in sub-queries. pruned_select_columns needs to be passed in since the
        node may have the select columns pruned.
        """
        all_expr_search_results: List[SqlExpressionTreeLineage] = []

        for select_column in pruned_select_columns:
            all_expr_search_results.append(select_column.expr.lineage)

        for join_description in select_node.join_descs:
            all_expr_search_results.append(join_description.on_condition.lineage)

        for group_by in select_node.group_bys:
            all_expr_search_results.append(group_by.expr.lineage)

        for order_by in select_node.order_bys:
            all_expr_search_results.append(order_by.expr.lineage)

        if select_node.where:
            all_expr_search_results.append(select_node.where.lineage)

        return SqlExpressionTreeLineage.combine(all_expr_search_results)

    def _prune_columns_from_grandparents(
        self, node: SqlSelectStatementNode, pruned_select_columns: Tuple[SqlSelectColumn, ...]
    ) -> SqlSelectStatementNode:
        """Assume that you need all columns from the parent and prune the grandparents."""
        pruned_from_source: SqlQueryPlanNode
        if node.from_source.as_select_node:
            from_visitor = SqlColumnPrunerVisitor(
                required_column_aliases={x.column_alias for x in node.from_source.as_select_node.select_columns}
            )
            pruned_from_source = node.from_source.as_select_node.accept(from_visitor)
        else:
            pruned_from_source = node.from_source
        pruned_join_descriptions: List[SqlJoinDescription] = []
        for join_description in node.join_descs:
            right_source_as_select_node = join_description.right_source.as_select_node
            if right_source_as_select_node:
                right_source_visitor = SqlColumnPrunerVisitor(
                    required_column_aliases={x.column_alias for x in right_source_as_select_node.select_columns}
                )
                pruned_join_descriptions.append(
                    SqlJoinDescription(
                        right_source=join_description.right_source.accept(right_source_visitor),
                        right_source_alias=join_description.right_source_alias,
                        on_condition=join_description.on_condition,
                        join_type=join_description.join_type,
                    )
                )
            else:
                pruned_join_descriptions.append(join_description)

        return SqlSelectStatementNode(
            description=node.description,
            select_columns=pruned_select_columns,
            from_source=pruned_from_source,
            from_source_alias=node.from_source_alias,
            joins_descs=tuple(pruned_join_descriptions),
            group_bys=node.group_bys,
            order_bys=node.order_bys,
            where=node.where,
            limit=node.limit,
        )

    def visit_select_statement_node(self, node: SqlSelectStatementNode) -> SqlQueryPlanNode:  # noqa: D
        # Remove columns that are not needed from this SELECT statement because the parent SELECT statement doesn't
        # need them. However, keep columns that are in group bys because that changes the meaning of the query.
        pruned_select_columns = tuple(
            select_column
            for select_column in node.select_columns
            if select_column.column_alias in self._required_column_aliases or select_column in node.group_bys
        )

        if len(pruned_select_columns) == 0:
            raise RuntimeError("All columns have been pruned - this indicates an bug in the pruner or in the inputs.")

        # Based on the expressions in this select statement, figure out what column aliases are needed in the sources of
        # this query (i.e. tables or sub-queries in the FROM or JOIN clauses).
        exprs_used_in_this_node = self._search_for_expressions(node, pruned_select_columns)

        # If any of the string expressions don't have context on what columns are used in the expression, then it's
        # impossible to know what columns can be pruned from the parent sources. So return a SELECT statement that
        # leaves the parent sources untouched. Columns from the grandparents can be pruned based on the parent node
        # though.
        if any([string_expr.used_columns is None for string_expr in exprs_used_in_this_node.string_exprs]):
            return self._prune_columns_from_grandparents(node, pruned_select_columns)

        # Create a mapping from the source alias to the column aliases needed from the corresponding source.
        source_alias_to_required_column_alias: Dict[str, Set[str]] = defaultdict(set)
        for column_reference_expr in exprs_used_in_this_node.column_reference_exprs:
            column_reference = column_reference_expr.col_ref
            source_alias_to_required_column_alias[column_reference.table_alias].add(column_reference.column_name)

        # For all string columns, assume that they are needed from all sources since we don't have a table alias
        # in SqlStringExpression.used_columns
        for string_expr in exprs_used_in_this_node.string_exprs:
            if string_expr.used_columns:
                for column_alias in string_expr.used_columns:
                    source_alias_to_required_column_alias[node.from_source_alias].add(column_alias)
                    for join_description in node.join_descs:
                        source_alias_to_required_column_alias[join_description.right_source_alias].add(column_alias)
        # Same with unqualified column references.
        for unqualified_column_reference_expr in exprs_used_in_this_node.column_alias_reference_exprs:
            column_alias = unqualified_column_reference_expr.column_alias
            source_alias_to_required_column_alias[node.from_source_alias].add(column_alias)
            for join_description in node.join_descs:
                source_alias_to_required_column_alias[join_description.right_source_alias].add(column_alias)

        # Once we know which column aliases are required from which source aliases, replace the sources with new SELECT
        # statements.
        from_source_pruner = SqlColumnPrunerVisitor(
            required_column_aliases=source_alias_to_required_column_alias[node.from_source_alias]
        )
        pruned_from_source = node.from_source.accept(from_source_pruner)
        pruned_join_descriptions: List[SqlJoinDescription] = []
        for join_description in node.join_descs:
            join_source_pruner = SqlColumnPrunerVisitor(
                required_column_aliases=source_alias_to_required_column_alias[join_description.right_source_alias]
            )
            pruned_join_descriptions.append(
                SqlJoinDescription(
                    right_source=join_description.right_source.accept(join_source_pruner),
                    right_source_alias=join_description.right_source_alias,
                    on_condition=join_description.on_condition,
                    join_type=join_description.join_type,
                )
            )

        return SqlSelectStatementNode(
            description=node.description,
            select_columns=tuple(pruned_select_columns),
            from_source=pruned_from_source,
            from_source_alias=node.from_source_alias,
            joins_descs=tuple(pruned_join_descriptions),
            group_bys=node.group_bys,
            order_bys=node.order_bys,
            where=node.where,
            limit=node.limit,
        )

    def visit_table_from_clause_node(self, node: SqlTableFromClauseNode) -> SqlQueryPlanNode:  # noqa: D
        """This node is effectively a FROM statement inside a SELECT statement node, so pruning cannot apply."""
        return node

    def visit_query_from_clause_node(self, node: SqlSelectQueryFromClauseNode) -> SqlQueryPlanNode:  # noqa: D
        """Pruning cannot be done here since this is an arbitrary user-provided SQL query."""
        return node


class SqlColumnPrunerOptimizer(SqlQueryPlanOptimizer):
    """Removes unnecessary columns in the SELECT clauses."""

    def optimize(self, node: SqlQueryPlanNode) -> SqlQueryPlanNode:  # noqa: D

        # Can't prune columns without knowing the structure of the query.
        if not node.as_select_node:
            return node

        pruning_visitor = SqlColumnPrunerVisitor(
            required_column_aliases={x.column_alias for x in node.as_select_node.select_columns}
        )

        return node.accept(pruning_visitor)
