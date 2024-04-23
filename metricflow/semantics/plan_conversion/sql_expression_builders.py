"""Utility module for building sql expressions from inputs derived from dataflow plan or other nodes."""
from __future__ import annotations

from typing import List, Sequence

from metricflow.sql.sql_exprs import (
    SqlAggregateFunctionExpression,
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlExpressionNode,
    SqlFunction,
)


def make_coalesced_expr(table_aliases: Sequence[str], column_alias: str) -> SqlExpressionNode:
    """Makes a coalesced expression of the given column from the given table aliases.

    e.g.

    table_aliases = ["a", "b"]
    column_alias = "is_instant"

    ->

    COALESCE(a.is_instant, b.is_instant)
    """
    if len(table_aliases) == 1:
        return SqlColumnReferenceExpression(
            col_ref=SqlColumnReference(
                table_alias=table_aliases[0],
                column_name=column_alias,
            )
        )
    else:
        columns_to_coalesce: List[SqlExpressionNode] = []
        for table_alias in table_aliases:
            columns_to_coalesce.append(
                SqlColumnReferenceExpression(
                    col_ref=SqlColumnReference(
                        table_alias=table_alias,
                        column_name=column_alias,
                    )
                )
            )
        return SqlAggregateFunctionExpression(
            sql_function=SqlFunction.COALESCE,
            sql_function_args=columns_to_coalesce,
        )
