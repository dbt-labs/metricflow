from __future__ import annotations

import logging
from collections import defaultdict
from typing import Dict, List, Set, Tuple

from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

from metricflow.sql.optimizer.tag_column_aliases import TaggedColumnAliasSet
from metricflow.sql.sql_exprs import SqlExpressionTreeLineage
from metricflow.sql.sql_plan import (
    SqlCreateTableAsNode,
    SqlCteNode,
    SqlQueryPlanNode,
    SqlQueryPlanNodeVisitor,
    SqlSelectColumn,
    SqlSelectQueryFromClauseNode,
    SqlSelectStatementNode,
    SqlTableNode,
)

logger = logging.getLogger(__name__)


class SqlTagRequiredColumnAliasesVisitor(SqlQueryPlanNodeVisitor[None]):
    """To aid column pruning, traverse the SQL-query representation DAG and tag all column aliases that are required.

    For example, for the query:

        SELECT source_0.col_0 AS col_0_renamed
        FROM (
            SELECT
                example_table.col_0
                example_table.col_1
            FROM example_table_0
        ) source_0

    The top-level SQL node would have the column alias `col_0_renamed` tagged, and the SQL node associated with
    `source_0` would have `col_0` tagged. Once tagged, the information can be used to prune the columns in the SELECT:

        SELECT source_0.col_0 AS col_0_renamed
        FROM (
            SELECT
                example_table.col_0
            FROM example_table_0
        ) source_0
    """

    def __init__(self, tagged_column_alias_set: TaggedColumnAliasSet) -> None:
        """Initializer.

        Args:
            tagged_column_alias_set: Stores the set of columns that are tagged. This will be updated as the visitor
            traverses the SQL-query representation DAG.
        """
        self._column_alias_tagger = tagged_column_alias_set

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
            if join_description.on_condition:
                all_expr_search_results.append(join_description.on_condition.lineage)

        for group_by in select_node.group_bys:
            all_expr_search_results.append(group_by.expr.lineage)

        for order_by in select_node.order_bys:
            all_expr_search_results.append(order_by.expr.lineage)

        if select_node.where:
            all_expr_search_results.append(select_node.where.lineage)

        return SqlExpressionTreeLineage.combine(all_expr_search_results)

    @override
    def visit_cte_node(self, node: SqlCteNode) -> None:
        raise NotImplementedError

    def _visit_parents(self, node: SqlQueryPlanNode) -> None:
        """Default recursive handler to visit the parents of the given node."""
        for parent_node in node.parent_nodes:
            parent_node.accept(self)
        return

    def visit_select_statement_node(self, node: SqlSelectStatementNode) -> None:  # noqa: D102
        # Based on column aliases that are tagged in this SELECT statement, tag corresponding column aliases in
        # parent nodes.

        initial_required_column_aliases_in_this_node = self._column_alias_tagger.get_tagged_aliases(node)

        # If this SELECT statement uses DISTINCT, all columns are required as removing them would change the meaning of
        # the query.
        updated_required_column_aliases_in_this_node = set(initial_required_column_aliases_in_this_node)
        if node.distinct:
            updated_required_column_aliases_in_this_node.update(
                {select_column.column_alias for select_column in node.select_columns}
            )

        # Any columns in the group by also need to be kept to have a correct query.
        updated_required_column_aliases_in_this_node.update(
            {group_by_select_column.column_alias for group_by_select_column in node.group_bys}
        )
        logger.debug(
            LazyFormat(
                "Tagging column aliases in parent nodes given what's required in this node",
                this_node=node,
                initial_required_column_aliases_in_this_node=list(initial_required_column_aliases_in_this_node),
                updated_required_column_aliases_in_this_node=list(updated_required_column_aliases_in_this_node),
            )
        )
        # Since additional select columns could have been selected due to DISTINCT or GROUP BY, re-tag.
        self._column_alias_tagger.tag_aliases(node, updated_required_column_aliases_in_this_node)

        required_select_columns_in_this_node = tuple(
            select_column
            for select_column in node.select_columns
            if select_column.column_alias in updated_required_column_aliases_in_this_node
        )

        # TODO: don't prune columns used in join condition! Tricky to derive since the join condition can be any
        # SqlExpressionNode.

        if len(required_select_columns_in_this_node) == 0:
            raise RuntimeError(
                "No columns are required in this node - this indicates a bug in this collector or in the inputs."
            )

        # Based on the expressions in this select statement, figure out what column aliases are needed in the sources of
        # this query (i.e. tables or sub-queries in the FROM or JOIN clauses).
        exprs_used_in_this_node = self._search_for_expressions(node, required_select_columns_in_this_node)

        # If any of the string expressions don't have context on what columns are used in the expression, then it's
        # impossible to know what columns can be pruned from the parent sources. Tag all columns in parents as required.
        if any([string_expr.used_columns is None for string_expr in exprs_used_in_this_node.string_exprs]):
            for parent_node in node.parent_nodes:
                self._column_alias_tagger.tag_all_aliases_in_node(parent_node)
            self._visit_parents(node)
            return

        # Create a mapping from the source alias to the column aliases needed from the corresponding source.
        source_alias_to_required_column_alias: Dict[str, Set[str]] = defaultdict(set)
        for column_reference_expr in exprs_used_in_this_node.column_reference_exprs:
            column_reference = column_reference_expr.col_ref
            source_alias_to_required_column_alias[column_reference.table_alias].add(column_reference.column_name)

        # Appropriately tag the columns required in the parent nodes.
        if node.from_source_alias in source_alias_to_required_column_alias:
            aliases_required_in_parent = source_alias_to_required_column_alias[node.from_source_alias]
            self._column_alias_tagger.tag_aliases(node=node.from_source, column_aliases=aliases_required_in_parent)
        for join_desc in node.join_descs:
            if join_desc.right_source_alias in source_alias_to_required_column_alias:
                aliases_required_in_parent = source_alias_to_required_column_alias[join_desc.right_source_alias]
                self._column_alias_tagger.tag_aliases(
                    node=join_desc.right_source, column_aliases=aliases_required_in_parent
                )
        # TODO: Handle CTEs parent nodes.

        # For all string columns, assume that they are needed from all sources since we don't have a table alias
        # in SqlStringExpression.used_columns
        for string_expr in exprs_used_in_this_node.string_exprs:
            if string_expr.used_columns:
                for column_alias in string_expr.used_columns:
                    for parent_node in node.parent_nodes:
                        self._column_alias_tagger.tag_alias(parent_node, column_alias)

        # Same with unqualified column references - it's hard to tell which source it came from, so it's safest to say
        # it's required from all parents.
        # An unqualified column reference expression is like `SELECT col_0` whereas a qualified column reference
        # expression is like `SELECT table_0.col_0`.
        for unqualified_column_reference_expr in exprs_used_in_this_node.column_alias_reference_exprs:
            column_alias = unqualified_column_reference_expr.column_alias
            for parent_node in node.parent_nodes:
                self._column_alias_tagger.tag_alias(parent_node, column_alias)

        # Visit recursively.
        self._visit_parents(node)
        return

    def visit_table_node(self, node: SqlTableNode) -> None:
        """There are no SELECT columns in this node, so pruning cannot apply."""
        return

    def visit_query_from_clause_node(self, node: SqlSelectQueryFromClauseNode) -> None:
        """Pruning cannot be done here since this is an arbitrary user-provided SQL query."""
        return

    def visit_create_table_as_node(self, node: SqlCreateTableAsNode) -> None:  # noqa: D102
        return self._visit_parents(node)
