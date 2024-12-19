from __future__ import annotations

import logging
from collections import defaultdict
from typing import Dict, FrozenSet, List, Set, Tuple

from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.sql.sql_exprs import SqlExpressionTreeLineage
from typing_extensions import override

from metricflow.sql.optimizer.tag_column_aliases import NodeToColumnAliasMapping
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


class SqlMapRequiredColumnAliasesVisitor(SqlQueryPlanNodeVisitor[None]):
    """To aid column pruning, traverse the SQL-query representation DAG and map the SELECT columns needed at each node.

    For example, the query:

        -- SELECT node_id="select_0"
        SELECT source_0.col_0 AS col_0_renamed
        FROM (
            -- SELECT node_id="select_1
            SELECT
                example_table.col_0
                example_table.col_1
            FROM example_table_0
        ) source_0

    would generate the mapping:

        {
            "select_0": {"col_0"},
            "select_1": {"col_0"),
        }

    The mapping can be later used to rewrite the query to:

        SELECT source_0.col_0 AS col_0_renamed
        FROM (
            SELECT
                example_table.col_0
            FROM example_table_0
        ) source_0
    """

    def __init__(self, start_node: SqlQueryPlanNode, required_column_aliases_in_start_node: FrozenSet[str]) -> None:
        """Initializer.

        Args:
            start_node: The node where the traversal by this visitor will start.
            required_column_aliases_in_start_node: The column aliases at the `start_node` that are required.
        """
        # Stores the mapping of the SQL node to the required column aliases. This will be updated as the visitor
        # traverses the SQL-query representation DAG.
        self._current_required_column_alias_mapping = NodeToColumnAliasMapping()
        self._current_required_column_alias_mapping.add_aliases(start_node, required_column_aliases_in_start_node)

        # Helps lookup the CTE node associated with a given CTE alias. A member variable is needed as any node in the
        # SQL DAG can reference a CTE.
        start_node_as_select_node = start_node.as_select_node
        self._cte_alias_to_cte_node: Dict[str, SqlCteNode] = (
            {cte_source.cte_alias: cte_source for cte_source in start_node_as_select_node.cte_sources}
            if start_node_as_select_node is not None
            else {}
        )

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

        return SqlExpressionTreeLineage.merge_iterable(all_expr_search_results)

    @override
    def visit_cte_node(self, node: SqlCteNode) -> None:
        select_statement = node.select_statement
        # Copy the tagged aliases from the CTE to the SELECT since when visiting a SELECT, the CTE node (not the SELECT
        # in the CTE) was tagged with the required aliases.
        required_column_aliases_in_this_node = self._current_required_column_alias_mapping.get_aliases(node)
        self._current_required_column_alias_mapping.add_aliases(select_statement, required_column_aliases_in_this_node)
        # Visit parent nodes.
        select_statement.accept(self)

    def _visit_parents(self, node: SqlQueryPlanNode) -> None:
        """Default recursive handler to visit the parents of the given node."""
        for parent_node in node.parent_nodes:
            parent_node.accept(self)
        return

    def _tag_potential_cte_node(self, table_name: str, column_aliases: Set[str]) -> None:
        """A reference to a SQL table might be a CTE. If so, tag the appropriate aliases in the CTEs."""
        cte_node = self._cte_alias_to_cte_node.get(table_name)
        if cte_node is not None:
            self._current_required_column_alias_mapping.add_aliases(cte_node, column_aliases)
            # `visit_cte_node` will handle propagating the required aliases to all CTEs that this CTE node depends on.
            cte_node.accept(self)

    def visit_select_statement_node(self, node: SqlSelectStatementNode) -> None:
        """Based on required column aliases for this SELECT, figure out required column aliases in parents."""
        initial_required_column_aliases_in_this_node = self._current_required_column_alias_mapping.get_aliases(node)

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
        self._current_required_column_alias_mapping.add_aliases(node, updated_required_column_aliases_in_this_node)

        required_select_columns_in_this_node = tuple(
            select_column
            for select_column in node.select_columns
            if select_column.column_alias in updated_required_column_aliases_in_this_node
        )

        if len(required_select_columns_in_this_node) == 0:
            raise RuntimeError(
                "No columns are required in this node - this indicates a bug in this visitor or in the inputs."
            )
        # It's possible for `required_select_columns_in_this_node` to be empty because we traverse through the ancestors
        # of a CTE node whenever a CTE node is updated. See `test_multi_child_pruning`.

        # Based on the expressions in this select statement, figure out what column aliases are needed in the sources of
        # this query (i.e. tables or sub-queries in the FROM or JOIN clauses).
        exprs_used_in_this_node = self._search_for_expressions(node, required_select_columns_in_this_node)

        # If any of the string expressions don't have context on what columns are used in the expression, then it's
        # impossible to know what columns can be pruned from the parent sources. Tag all columns in parents as required.
        if any([string_expr.used_columns is None for string_expr in exprs_used_in_this_node.string_exprs]):
            nodes_to_retain_all_columns = [node.from_source]
            for join_desc in node.join_descs:
                nodes_to_retain_all_columns.append(join_desc.right_source)

            for node_to_retain_all_columns in nodes_to_retain_all_columns:
                nearest_select_columns = node_to_retain_all_columns.nearest_select_columns(self._cte_alias_to_cte_node)
                for select_column in nearest_select_columns or ():
                    self._current_required_column_alias_mapping.add_alias(
                        node=node_to_retain_all_columns, column_alias=select_column.column_alias
                    )

            self._visit_parents(node)
            return

        # Create a mapping from the source alias to the column aliases needed from the corresponding source.
        source_alias_to_required_column_aliases: Dict[str, Set[str]] = defaultdict(set)
        for column_reference_expr in exprs_used_in_this_node.column_reference_exprs:
            column_reference = column_reference_expr.col_ref
            source_alias_to_required_column_aliases[column_reference.table_alias].add(column_reference.column_name)

        logger.debug(
            LazyFormat(
                "Collected required column names from sources",
                source_alias_to_required_column_aliases=source_alias_to_required_column_aliases,
            )
        )
        # Appropriately tag the columns required in the parent nodes.
        if node.from_source_alias in source_alias_to_required_column_aliases:
            aliases_required_in_parent = source_alias_to_required_column_aliases[node.from_source_alias]
            self._current_required_column_alias_mapping.add_aliases(
                node=node.from_source, column_aliases=aliases_required_in_parent
            )
            from_source_as_sql_table_node = node.from_source.as_sql_table_node
            if from_source_as_sql_table_node is not None:
                self._tag_potential_cte_node(
                    table_name=from_source_as_sql_table_node.sql_table.table_name,
                    column_aliases=aliases_required_in_parent,
                )
        for join_desc in node.join_descs:
            if join_desc.right_source_alias in source_alias_to_required_column_aliases:
                aliases_required_in_parent = source_alias_to_required_column_aliases[join_desc.right_source_alias]
                self._current_required_column_alias_mapping.add_aliases(
                    node=join_desc.right_source, column_aliases=aliases_required_in_parent
                )
                right_source_as_sql_table_node = join_desc.right_source.as_sql_table_node
                if right_source_as_sql_table_node is not None:
                    self._tag_potential_cte_node(
                        table_name=right_source_as_sql_table_node.sql_table.table_name,
                        column_aliases=aliases_required_in_parent,
                    )

        # For all string columns, assume that they are needed from all sources since we don't have a table alias
        # in SqlStringExpression.used_columns
        for string_expr in exprs_used_in_this_node.string_exprs:
            if string_expr.used_columns:
                for column_alias in string_expr.used_columns:
                    for node_to_retain_all_columns in (node.from_source,) + tuple(
                        join_desc.right_source for join_desc in node.join_descs
                    ):
                        self._current_required_column_alias_mapping.add_alias(node_to_retain_all_columns, column_alias)

        # Same with unqualified column references - it's hard to tell which source it came from, so it's safest to say
        # it's required from all parents.
        # An unqualified column reference expression is like `SELECT col_0` whereas a qualified column reference
        # expression is like `SELECT table_0.col_0`.
        for unqualified_column_reference_expr in exprs_used_in_this_node.column_alias_reference_exprs:
            column_alias = unqualified_column_reference_expr.column_alias
            for node_to_retain_all_columns in (node.from_source,) + tuple(
                join_desc.right_source for join_desc in node.join_descs
            ):
                self._current_required_column_alias_mapping.add_alias(node_to_retain_all_columns, column_alias)

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

    @property
    def required_column_alias_mapping(self) -> NodeToColumnAliasMapping:
        """Return the column aliases required at each node as determined after traversal."""
        return self._current_required_column_alias_mapping
