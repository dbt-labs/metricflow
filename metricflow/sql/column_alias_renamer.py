from __future__ import annotations

import logging
from collections.abc import Mapping

from metricflow_semantics.sql.sql_exprs import SqlColumnReference, SqlColumnReferenceExpression

from metricflow.sql.sql_plan import SqlSelectColumn
from metricflow.sql.sql_select_node import SqlSelectStatementNode

logger = logging.getLogger(__name__)


class ColumnAliasRenamer:
    """Helper to rename column aliases in a SQL `SELECT` statement."""

    def rename(
        self,
        select_statement_node: SqlSelectStatementNode,
        previous_column_alias_to_next_column_alias: Mapping[str, str],
    ) -> SqlSelectStatementNode:
        """Creates a new `SELECT` statement that changes the column aliases.

        Example:
            SELECT src.col AS col_0 FROM src_table src

            ->

            SELECT src.col AS col_1 FROM src_table src
        """
        return select_statement_node.with_select_columns(
            select_column.copy_with_new_alias(previous_column_alias_to_next_column_alias[select_column.column_alias])
            for select_column in select_statement_node.select_columns
        )

    def rename_via_subquery(
        self,
        select_statement_node: SqlSelectStatementNode,
        previous_column_name_to_next_column_name: Mapping[str, str],
        description: str,
        inner_query_alias: str,
    ) -> SqlSelectStatementNode:
        """Creates a new `SELECT` statement that changes the column identifiers via a subquery.

        A subquery is used so that a `WHERE` clause that references the modified names can be added to the outer
        query.

        Example:
            SELECT src.col AS col_0 FROM src_table src

            ->

            SELECT subq.col_1 AS col_1
            FROM (
                SELECT src.col AS col_1
            ) subq

        Otherwise, a query like:

            SELECT src.col AS col_1
            WHERE col_1 IS NOT NULL

        will fail in some SQL engines due to the `WHERE` clause being evaluated first.
        """
        outer_select_node_columns = []
        for next_column_name in previous_column_name_to_next_column_name.values():
            outer_select_node_columns.append(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        SqlColumnReference(
                            table_alias=inner_query_alias,
                            column_name=next_column_name,
                        )
                    ),
                    column_alias=next_column_name,
                )
            )

        inner_select_node = select_statement_node.with_select_columns(
            select_column.copy_with_new_alias(previous_column_name_to_next_column_name[select_column.column_alias])
            for select_column in select_statement_node.select_columns
        )
        return SqlSelectStatementNode.create(
            description=description,
            select_columns=outer_select_node_columns,
            from_source=inner_select_node,
            from_source_alias=inner_query_alias,
        )
