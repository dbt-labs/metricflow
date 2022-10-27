from dataclasses import dataclass
from typing import List, Sequence

from metricflow.sql.sql_plan import SqlExpressionNode, SqlJoinDescription, SqlJoinType, SqlSelectStatementNode
from metricflow.sql.sql_exprs import (
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlComparison,
    SqlComparisonExpression,
    SqlLogicalExpression,
    SqlLogicalOperator,
)


@dataclass(frozen=True)
class ColumnEqualityDescription:
    """Helper class to enumerate columns that should be equal between sources in a join."""

    left_column_alias: str
    right_column_alias: str


class SqlQueryPlanJoinBuilder:
    """Helper class for constructing various join components in a SqlQueryPlan"""

    @staticmethod
    def make_sql_join_description(
        right_source_node: SqlSelectStatementNode,
        left_source_alias: str,
        right_source_alias: str,
        column_equality_descriptions: Sequence[ColumnEqualityDescription],
        join_type: SqlJoinType,
        additional_on_conditions: Sequence[SqlExpressionNode] = tuple(),
    ) -> SqlJoinDescription:
        """Make a join description where the condition is a set of equality comparisons between columns.

        Typically the columns in column_equality_descriptions are identifiers we are trying to match,
        although they may include things like dimension partitions or time dimension columns where an
        equality is expected.

        Args:
            right_source_node: node representing the join target, may be either a table or subquery
            left_source_alias: string alias identifier for the join source
            right_source_alias: string alias identifier for the join target
            column_equality_descriptions: set of equality constraints for the ON statement
            join_type: type of SQL join, e.g., LEFT, INNER, etc.
            additional_on_conditions: set of additional constraints to add in the ON statement (via AND)
        """
        assert len(column_equality_descriptions) > 0

        and_conditions: List[SqlExpressionNode] = []
        for column_equality_description in column_equality_descriptions:
            and_conditions.append(
                SqlComparisonExpression(
                    left_expr=SqlColumnReferenceExpression(
                        SqlColumnReference(
                            table_alias=left_source_alias,
                            column_name=column_equality_description.left_column_alias,
                        )
                    ),
                    comparison=SqlComparison.EQUALS,
                    right_expr=SqlColumnReferenceExpression(
                        SqlColumnReference(
                            table_alias=right_source_alias,
                            column_name=column_equality_description.right_column_alias,
                        )
                    ),
                )
            )
        and_conditions += additional_on_conditions

        on_condition: SqlExpressionNode
        if len(and_conditions) == 1:
            on_condition = and_conditions[0]
        else:
            on_condition = SqlLogicalExpression(operator=SqlLogicalOperator.AND, args=tuple(and_conditions))

        return SqlJoinDescription(
            right_source=right_source_node,
            right_source_alias=right_source_alias,
            on_condition=on_condition,
            join_type=join_type,
        )
