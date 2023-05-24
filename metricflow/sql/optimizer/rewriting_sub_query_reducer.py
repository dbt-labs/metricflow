from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

from metricflow.sql.optimizer.sql_query_plan_optimizer import SqlQueryPlanOptimizer
from metricflow.sql.sql_exprs import (
    SqlColumnAliasReferenceExpression,
    SqlColumnReference,
    SqlColumnReplacements,
    SqlExpressionNode,
    SqlExpressionTreeLineage,
    SqlLogicalExpression,
    SqlLogicalOperator,
)
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


@dataclass
class RewritableSqlClauses:
    """Stores clauses in a SQL query that should be rewritten when a node is reduced."""

    select_columns: List[SqlSelectColumn]
    wheres: List[SqlExpressionNode]
    group_bys: List[SqlSelectColumn]
    order_bys: List[SqlOrderByDescription]

    def rewrite(self, column_replacements: SqlColumnReplacements) -> None:
        """Rewrite all clauses using the given replacements."""
        self.select_columns = [
            SqlSelectColumn(expr=x.expr.rewrite(column_replacements), column_alias=x.column_alias)
            for x in self.select_columns
        ]
        self.wheres = [x.rewrite(column_replacements=column_replacements) for x in self.wheres]
        self.group_bys = [
            SqlSelectColumn(expr=x.expr.rewrite(column_replacements), column_alias=x.column_alias)
            for x in self.group_bys
        ]
        self.order_bys = [
            SqlOrderByDescription(
                expr=x.expr.rewrite(column_replacements=column_replacements),
                desc=x.desc,
            )
            for x in self.order_bys
        ]

    def combine_wheres(self, additional_where_clauses: List[SqlExpressionNode]) -> Optional[SqlExpressionNode]:
        """Combine the WHERE clauses in this with the additional clauses to form a single WHERE clause."""
        all_where_clauses = self.wheres + additional_where_clauses
        if len(all_where_clauses) == 1:
            return all_where_clauses[0]
        elif len(all_where_clauses) > 1:
            return SqlLogicalExpression(
                operator=SqlLogicalOperator.AND,
                args=tuple(all_where_clauses),
            )
        return None

    @property
    def contains_ambiguous_exprs(self) -> bool:
        """Returns true if any of the clauses have ambiguous expressions that will be difficult to re-write."""
        return any(
            [x.expr.lineage.contains_ambiguous_exprs for x in self.select_columns]
            + [x.lineage.contains_ambiguous_exprs for x in self.wheres]
            + [x.expr.lineage.contains_ambiguous_exprs for x in self.group_bys]
            + [x.expr.lineage.contains_ambiguous_exprs for x in self.order_bys]
        )


class SqlRewritingSubQueryReducerVisitor(SqlQueryPlanNodeVisitor[SqlQueryPlanNode]):
    """Visits the SQL query plan to simplify sub-queries. On each visit, return a simplified node.

    Unlike SqlSubQueryReducerVisitor, this will re-write expressions to realize more reductions.
    """

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

    @staticmethod
    def _statement_contains_difficult_expressions(node: SqlSelectStatementNode) -> bool:
        combined_lineage = SqlExpressionTreeLineage.combine(
            tuple(x.expr.lineage for x in node.select_columns)
            + ((node.where.lineage,) if node.where else ())
            + tuple(x.expr.lineage for x in node.group_bys)
            + tuple(x.expr.lineage for x in node.order_bys)
        )

        return combined_lineage.contains_string_exprs or combined_lineage.contains_column_alias_exprs

    @staticmethod
    def _select_columns_contain_string_expressions(select_columns: Tuple[SqlSelectColumn, ...]) -> bool:
        combined_lineage = SqlExpressionTreeLineage.combine(tuple(x.expr.lineage for x in select_columns))

        return len(combined_lineage.string_exprs) > 0

    @staticmethod
    def _select_columns_are_column_references(select_columns: Tuple[SqlSelectColumn, ...]) -> bool:
        for select_column in select_columns:
            if not select_column.expr.as_column_reference_expression:
                return False
        return True

    @staticmethod
    def _select_column_for_alias(column_alias: str, select_columns: Sequence[SqlSelectColumn]) -> SqlSelectColumn:
        for select_column in select_columns:
            if select_column.column_alias == column_alias:
                return select_column
        raise RuntimeError(f"Column alias '{column_alias}' not in SELECT columns: {select_columns}")

    @staticmethod
    def _is_simple_source(node: SqlSelectStatementNode) -> bool:
        """Returns true if the node is simple.

        Simple is defined as having no JOINs, WHERE, GROUP BYs, ORDER BYs, LIMIT, AGG functions, and there are no strings in the column
        select. Strings are avoided so that the child node doesn't use the string expression in a group by or cause
        aliasing issues when used in the child query. Aggregate functions are avoided due to the nature of applying on grouped rows which
        is essentially the effect as group bys and should be treated in here as such.

        e.g.

        Reducing

        SELECT
          a.col AS a_col
          , b.col AS b_col
        FROM (
           SELECT
             col AS col -- "col" is a string expression, not a column reference expression.
             , src0.join_key AS join_key
           FROM foo src0
        ) a
        JOIN b
        ON a.join_key = b.join_key

        to

        SELECT
          col AS a_col
          , b.col AS b_col
        FROM foo src0
        JOIN b
        ON a.join_key = b.join_key

        will throw an ambiguous column error on "col".
        """
        for select_column in node.select_columns:
            if select_column.expr.lineage.contains_string_exprs:
                return False
            if select_column.expr.lineage.contains_column_alias_exprs:
                return False
            if select_column.expr.lineage.contains_aggregate_exprs:
                return False
        return (
            len(node.parent_nodes) <= 1
            and len(node.group_bys) == 0
            and len(node.order_bys) == 0
            and not node.limit
            and not node.where
        )

    def _current_node_can_be_reduced(self, node: SqlSelectStatementNode) -> bool:  # noqa: D
        """Returns true if the given node can be reduced with the parent node.

        Reducing this node means eliminating the SELECT of this node and merging it with the parent SELECT. This
        checks for the cases where we are able to reduce.
        """
        # If this node has multiple parents (i.e. a join) that are complex, then this can't be collapsed.
        is_join = len(node.join_descs) > 0
        has_multiple_parent_nodes = len(node.parent_nodes) > 1
        if has_multiple_parent_nodes or is_join:
            return False

        assert len(node.parent_nodes) == 1
        parent_node = node.parent_nodes[0]

        # If the parent node is not a SELECT statement, then this can't be collapsed. e.g. with a table as a parent like
        # SELECT foo FROM bar
        if not parent_node.as_select_node:
            return False
        parent_select_node = parent_node.as_select_node

        # More conditions where we don't want to collapse. It's not impossible with these cases, but not reducing in
        # these cases for simplicity.

        # Re-writing string expressions / column alias expressions not yet supported, so don't reduce in those cases.
        if SqlRewritingSubQueryReducerVisitor._statement_contains_difficult_expressions(node):
            return False

        # Skip this case for simplicity of reasoning.
        if len(node.order_bys) > 0 and len(parent_select_node.order_bys) > 0:
            return False

        # Skip this case for simplicity of reasoning.
        if len(parent_select_node.group_bys) > 0 and len(node.group_bys) > 0:
            return False

        # TODO: Check for the following case:
        # SELECT
        #   bookings
        #   , 2 * bookings AS twice_bookings
        # FROM (
        #   SELECT,
        #     SUM(bookings) AS bookings
        #     , fct_bookings_src.is_instant
        #   FROM (
        #     SELECT * FROM demo.fct_bookings
        #   ) fct_bookings_src
        #   GROUP BY fct_bookings_src.is_instant
        # ) src
        #
        # If this is reduced, then the GROUP BY will refer to an unused column.

        # Don't reduce if the ORDER BYs aren't column reference expressions for simplicity.
        for order_by in node.order_bys:
            order_by_column_reference_expression = order_by.expr.as_column_reference_expression
            if not order_by_column_reference_expression:
                return False
            # Also for simplicity, the ORDER BY must match one of the SELECT expressions.
            if not SqlRewritingSubQueryReducerVisitor._find_matching_select_column(
                col_ref=order_by_column_reference_expression.col_ref,
                select_columns=node.select_columns,
            ):
                return False

        # If the parent has a GROUP BY and this has a WHERE, avoid reducing as the WHERE could reference an
        # aggregation expression.
        if len(parent_select_node.group_bys) > 0 and node.where:
            return False

        # If the parent has a GROUP BY, the case where it's easiest to merge this with the parent is if all select
        # columns are column references.
        if len(
            parent_select_node.group_bys
        ) > 0 and not SqlRewritingSubQueryReducerVisitor._select_columns_are_column_references(node.select_columns):
            return False

        # If the parent select node contains string columns, and this has a GROUP BY, don't reduce as string columns
        # can have special meanings in a GROUP BY. For example,
        #
        # SELECT
        #   a.col1 AS col1
        #   , a.col2 AS col2
        # FROM (
        #   SELECT
        #     b.col1 AS col1
        #     , 1 AS col2
        #   FROM foo b
        # ) a
        # GROUP BY a.col2
        #
        # would be rewritten in the current algorithm to
        #
        # SELECT
        #   b.col1 AS col1
        #   , 1 AS col2
        # FROM foo b
        # GROUP by 1
        #
        # However, the GROUP BY would be wrong as the "1" string expression would be interpreted as selecting the 1st
        # item in the SELECT to group by.

        if len(node.group_bys) > 0 and SqlRewritingSubQueryReducerVisitor._select_columns_contain_string_expressions(
            select_columns=parent_select_node.select_columns,
        ):
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

    @staticmethod
    def _get_column_replacements(parent_node: SqlSelectStatementNode, parent_node_alias: str) -> SqlColumnReplacements:
        column_replacements = {}
        for select_column in parent_node.select_columns:
            column_reference = SqlColumnReference(
                table_alias=parent_node_alias,
                column_name=select_column.column_alias,
            )
            column_replacements[column_reference] = select_column.expr

        return SqlColumnReplacements(column_replacements)

    @staticmethod
    def _rewrite_select_columns(
        old_select_columns: Tuple[SqlSelectColumn, ...], column_replacements: SqlColumnReplacements
    ) -> Tuple[SqlSelectColumn, ...]:
        return tuple(
            SqlSelectColumn(expr=x.expr.rewrite(column_replacements), column_alias=x.column_alias)
            for x in old_select_columns
        )

    @staticmethod
    def _rewrite_where(
        column_replacements: SqlColumnReplacements,
        node_where: Optional[SqlExpressionNode] = None,
        parent_node_where: Optional[SqlExpressionNode] = None,
    ) -> Optional[SqlExpressionNode]:
        if node_where is None and parent_node_where is None:
            return None
        elif node_where and parent_node_where is None:
            return node_where.rewrite(column_replacements)
        elif node_where is None and parent_node_where:
            return parent_node_where

        # For type checking. The above conditionals should ensure the below.
        assert node_where
        assert parent_node_where
        return SqlLogicalExpression(operator=SqlLogicalOperator.AND, args=(node_where, parent_node_where))

    @staticmethod
    def _find_matching_select_column(
        col_ref: SqlColumnReference, select_columns: Sequence[SqlSelectColumn]
    ) -> Optional[SqlSelectColumn]:
        for select_column in select_columns:
            column_reference_expression = select_column.expr.as_column_reference_expression
            if column_reference_expression and column_reference_expression.col_ref == col_ref:
                return select_column
        return None

    @staticmethod
    def _rewrite_node_with_join(node: SqlSelectStatementNode) -> SqlSelectStatementNode:
        """Reduces nodes with joins if the join source is simple to reduce.

        Converts this:

        SELECT
          SUM(bookings_src.bookings) AS bookings
          listings_src.country_latest AS listing__country_latest
          bookings_src.ds AS ds
        FROM (
          SELECT
            fct_bookings_src.booking AS bookings
            , fct_bookings_src.ds AS ds
            , fct_bookings_src.listing_id AS listing
          FROM demo.fct_bookings fct_bookings_src
        ) bookings_src
        JOIN (
          SELECT
            dim_listings_src.country_latest AS country_latest
            , listing_id AS listing
          FROM demo.dim_listings dim_listings_src
        ) listings_src
        ON bookings_src.listing = listings_src.listing
        GROUP BY bookings_src.ds

        to:

        SELECT
          SUM(bookings_src.booking) AS bookings
          dim_listings_src.country_latest AS listing__country_latest
          bookings_src.ds AS ds
        FROM demo.fct_bookings fct_bookings_src
        JOIN demo.dim_listings dim_listings_src
        ON bookings_src.listing_id = dim_listings_src.listing_id
        GROUP BY bookings_src.ds
        """
        # Check that there aren't any duplicates in source aliases, or else there would be a collision when reduced.
        # This check is conservative as it checks for duplicates in this node and parent nodes, but depending on
        # on which sources get reduced, there may not be a collision.
        from_source = node.from_source
        from_source_select = from_source.as_select_node
        from_source_alias = node.from_source_alias

        all_source_aliases = [from_source_alias]
        source_alias_set = {from_source_alias}

        if from_source_select:
            all_source_aliases.append(from_source_select.from_source_alias)
            source_alias_set.add(from_source_select.from_source_alias)

        for join_desc in node.join_descs:
            all_source_aliases.append(join_desc.right_source_alias)
            source_alias_set.add(join_desc.right_source_alias)
            joined_node_select = join_desc.right_source.as_select_node
            if joined_node_select:
                all_source_aliases.append(joined_node_select.from_source_alias)
                source_alias_set.add(joined_node_select.from_source_alias)

        # Some duplicate aliases, so safer to not reduce.
        if len(all_source_aliases) != len(source_alias_set):
            return node

        # See if any of the joined sources can be reduced.
        new_join_descs = []

        clauses_to_rewrite = RewritableSqlClauses(
            select_columns=list(node.select_columns),
            wheres=[node.where] if node.where else [],
            group_bys=list(node.group_bys),
            order_bys=list(node.order_bys),
        )

        # Can't re-write ambiguous expressions.
        if clauses_to_rewrite.contains_ambiguous_exprs:
            return node

        additional_where_clauses = []
        column_replacements_from_all_joins = []

        for join_desc in node.join_descs:
            join_select_node = join_desc.right_source.as_select_node

            # Verifying that it's simple makes it easier to reason about the logic.
            if not join_select_node or not SqlRewritingSubQueryReducerVisitor._is_simple_source(join_select_node):
                new_join_descs.append(join_desc)
                continue

            column_replacements = SqlRewritingSubQueryReducerVisitor._get_column_replacements(
                parent_node=join_select_node,
                parent_node_alias=join_desc.right_source_alias,
            )
            column_replacements_from_all_joins.append(column_replacements)

            new_join_descs.append(
                SqlJoinDescription(
                    right_source=join_select_node.from_source,
                    right_source_alias=join_select_node.from_source_alias,
                    on_condition=join_desc.on_condition.rewrite(column_replacements)
                    if join_desc.on_condition
                    else None,
                    join_type=join_desc.join_type,
                )
            )

            if join_select_node.where:
                additional_where_clauses.append(join_select_node.where)

            clauses_to_rewrite.rewrite(column_replacements)

        # While the above re-wrote each individual join when that join was reduced, the ON condition could reference
        # columns from other joins, so the column replacements have to be applied to all join ON conditions.
        for column_replacements in column_replacements_from_all_joins:
            new_join_descs = [
                SqlJoinDescription(
                    right_source=x.right_source,
                    right_source_alias=x.right_source_alias,
                    on_condition=x.on_condition.rewrite(column_replacements=column_replacements)
                    if x.on_condition
                    else None,
                    join_type=x.join_type,
                )
                for x in new_join_descs
            ]

        from_source_is_simple = (
            SqlRewritingSubQueryReducerVisitor._is_simple_source(from_source_select) if from_source_select else False
        )
        if from_source_select and from_source_is_simple:
            column_replacements = SqlRewritingSubQueryReducerVisitor._get_column_replacements(
                parent_node=from_source_select,
                parent_node_alias=node.from_source_alias,
            )

            if from_source_select.where:
                additional_where_clauses.append(from_source_select.where)

            clauses_to_rewrite.rewrite(column_replacements=column_replacements)
            # This was already checked in _is_simple_source().
            assert len(from_source_select.parent_nodes) == 1
            from_source = from_source_select.from_source
            from_source_alias = from_source_select.from_source_alias

            new_join_descs = [
                SqlJoinDescription(
                    right_source=x.right_source,
                    right_source_alias=x.right_source_alias,
                    on_condition=x.on_condition.rewrite(column_replacements=column_replacements)
                    if x.on_condition
                    else None,
                    join_type=x.join_type,
                )
                for x in new_join_descs
            ]

        return SqlSelectStatementNode(
            description=node.description,
            select_columns=tuple(clauses_to_rewrite.select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
            joins_descs=tuple(new_join_descs),
            group_bys=tuple(clauses_to_rewrite.group_bys),
            order_bys=tuple(clauses_to_rewrite.order_bys),
            where=clauses_to_rewrite.combine_wheres(additional_where_clauses),
            limit=node.limit,
        )

    def visit_select_statement_node(self, node: SqlSelectStatementNode) -> SqlQueryPlanNode:  # noqa: D
        node_with_reduced_parents = self._reduce_parents(node)

        if len(node_with_reduced_parents.parent_nodes) > 1:
            return SqlRewritingSubQueryReducerVisitor._rewrite_node_with_join(node_with_reduced_parents)

        if not self._current_node_can_be_reduced(node_with_reduced_parents):
            return node_with_reduced_parents

        # Note that just because this node can't be reduced doesn't mean the parent node can't. In this example, the
        # outer query can't be reduced because there is a join, but sub-query d can be.
        #
        # SELECT a.bookings, b.listing__country
        # FROM (
        #   SELECT d.bookings, d.listing_id
        #   FROM (
        #     SELECT e.bookings, e.listing_id
        #     FROM fct_bookings
        #   ) d
        #   LIMIT 10
        # ) b
        # JOIN dim_listings c
        # ON a.listing_id = b.listing_id

        assert len(node_with_reduced_parents.parent_nodes) == 1
        parent_node = node_with_reduced_parents.parent_nodes[0]
        parent_select_node = parent_node.as_select_node
        assert parent_select_node

        # At this point, the query should look similar to
        #
        #     SELECT b.foo, b.baz
        #     FROM (
        #       SELECT 1 AS foo, a.baz FROM bar a
        #       LIMIT 10
        #     ) b
        #     GROUP BY b.foo
        #     ORDER by b.baz
        #     LIMIT 1

        # The ORDER BY in the parent doesn't matter since the order by in this node will "overwrite" the order in the
        # parent as long as the parent has no limits.
        column_replacements = SqlRewritingSubQueryReducerVisitor._get_column_replacements(
            parent_node=parent_select_node,
            parent_node_alias=node.from_source_alias,
        )
        new_order_bys: List[SqlOrderByDescription] = []
        # Handle ORDER BY differently to avoid this hazard with expression rewriting:
        #
        # SELECT a.bookings AS bookings
        # FROM (
        #   SELECT SUM(b.bookings) AS bookings
        #   FROM fct_bookings b
        # ) a
        # ORDER BY a.bookings
        #
        # ->
        #
        # SELECT SUM(b.bookings) AS bookings
        # FROM fct_bookings b
        # ORDER BY SUM(b.bookings) -- Throws an error in some engines.
        if node_with_reduced_parents.order_bys:
            for order_by_item in node_with_reduced_parents.order_bys:
                order_by_item_expr = order_by_item.expr.as_column_reference_expression
                assert order_by_item_expr

                matching_select_column = SqlRewritingSubQueryReducerVisitor._find_matching_select_column(
                    col_ref=order_by_item_expr.col_ref,
                    select_columns=node_with_reduced_parents.select_columns,
                )
                # This must be the case because of _should_reduce()
                assert matching_select_column

                new_order_bys.append(
                    SqlOrderByDescription(
                        expr=SqlColumnAliasReferenceExpression(column_alias=matching_select_column.column_alias),
                        desc=order_by_item.desc,
                    )
                )

        # The limit should be the min of this SELECT limit and the parent SELECT limit.
        new_limit: Optional[int] = node_with_reduced_parents.limit
        if new_limit is None:
            new_limit = parent_select_node.limit
        elif parent_select_node.limit is not None:
            new_limit = min(new_limit, parent_select_node.limit)

        new_group_bys: Tuple[SqlSelectColumn, ...] = ()
        if node.group_bys and parent_select_node.group_bys:
            raise RuntimeError(
                "Attempting to reduce sub-queries when this and the parent have GROUP BYs. This should have been "
                "prevent by _should_reduce()"
            )
        elif node.group_bys:
            new_group_bys = SqlRewritingSubQueryReducerVisitor._rewrite_select_columns(
                old_select_columns=node.group_bys, column_replacements=column_replacements
            )
        elif parent_select_node.group_bys:
            new_group_bys = parent_select_node.group_bys

        return SqlSelectStatementNode(
            description="\n".join([parent_select_node.description, node_with_reduced_parents.description]),
            select_columns=SqlRewritingSubQueryReducerVisitor._rewrite_select_columns(
                old_select_columns=node.select_columns, column_replacements=column_replacements
            ),
            from_source=parent_select_node.from_source,
            from_source_alias=parent_select_node.from_source_alias,
            joins_descs=parent_select_node.join_descs,
            group_bys=new_group_bys,
            order_bys=tuple(new_order_bys),
            where=SqlRewritingSubQueryReducerVisitor._rewrite_where(
                column_replacements=column_replacements,
                node_where=node.where,
                parent_node_where=parent_select_node.where,
            ),
            limit=new_limit,
        )

    def visit_table_from_clause_node(self, node: SqlTableFromClauseNode) -> SqlQueryPlanNode:  # noqa: D
        return node

    def visit_query_from_clause_node(self, node: SqlSelectQueryFromClauseNode) -> SqlQueryPlanNode:  # noqa: D
        return node


class SqlGroupByRewritingVisitor(SqlQueryPlanNodeVisitor[SqlQueryPlanNode]):
    """Re-writes the GROUP BY to use a SqlColumnAliasReferenceExpression."""

    @staticmethod
    def _find_matching_select(
        expr: SqlExpressionNode, select_columns: Sequence[SqlSelectColumn]
    ) -> Optional[SqlSelectColumn]:
        """Given an expression, find the SELECT column that has the same expression."""
        for select_column in select_columns:
            if select_column.expr == expr:
                return select_column
        return None

    def visit_select_statement_node(self, node: SqlSelectStatementNode) -> SqlQueryPlanNode:  # noqa: D
        new_group_bys = []
        for group_by in node.group_bys:
            matching_select_column = SqlGroupByRewritingVisitor._find_matching_select(
                group_by.expr, node.select_columns
            )
            if matching_select_column:
                new_group_bys.append(
                    SqlSelectColumn(
                        expr=SqlColumnAliasReferenceExpression(column_alias=matching_select_column.column_alias),
                        column_alias=matching_select_column.column_alias,
                    )
                )
            else:
                logger.error(f"Did not find matching select for {group_by} in {node}")
                new_group_bys.append(group_by)

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
            group_bys=tuple(new_group_bys),
            order_bys=node.order_bys,
            where=node.where,
            limit=node.limit,
        )

    def visit_table_from_clause_node(self, node: SqlTableFromClauseNode) -> SqlQueryPlanNode:  # noqa: D
        return node

    def visit_query_from_clause_node(self, node: SqlSelectQueryFromClauseNode) -> SqlQueryPlanNode:  # noqa: D
        return node


class SqlRewritingSubQueryReducer(SqlQueryPlanOptimizer):
    """Simplify queries by eliminating sub-queries when possible by rewriting expressions.

     Expressions in the SELECT, GROUP BY, and WHERE are can be rewritten.

    e.g. from

    SELECT b.col0 AS foo
    FROM (
      SELECT SUM(a.col0) AS bar
      FROM table0 a
    ) b
    GROUP BY b.col0

    to

    SELECT SUM(a.col0) AS foo
    FROM table0 a
    GROUP BY foo
    """

    def __init__(self, use_column_alias_in_group_bys: bool = False) -> None:  # noqa: D
        self._use_column_alias_in_group_bys = use_column_alias_in_group_bys

    def optimize(self, node: SqlQueryPlanNode) -> SqlQueryPlanNode:  # noqa: D
        result = node.accept(SqlRewritingSubQueryReducerVisitor())
        if self._use_column_alias_in_group_bys:
            return result.accept(SqlGroupByRewritingVisitor())
        return result
