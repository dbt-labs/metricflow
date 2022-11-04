from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence, TypeVar

from metricflow.dataflow.dataflow_plan import JoinOverTimeRangeNode
from metricflow.plan_conversion.sql_dataset import SqlDataSet
from metricflow.sql.sql_plan import SqlExpressionNode, SqlJoinDescription, SqlJoinType, SqlSelectStatementNode
from metricflow.sql.sql_exprs import (
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlComparison,
    SqlComparisonExpression,
    SqlDateTruncExpression,
    SqlLogicalExpression,
    SqlLogicalOperator,
    SqlTimeDeltaExpression,
)

SqlDataSetT = TypeVar("SqlDataSetT", bound=SqlDataSet)


@dataclass(frozen=True)
class ColumnEqualityDescription:
    """Helper class to enumerate columns that should be equal between sources in a join."""

    left_column_alias: str
    right_column_alias: str


@dataclass(frozen=True)
class AnnotatedSqlDataSet:
    """Class to bind a DataSet to transient properties associated with it at a given point in the SqlQueryPlan"""

    data_set: SqlDataSet
    alias: str
    _metric_time_column_name: Optional[str]

    @property
    def metric_time_column_name(self) -> str:
        """Direct accessor for the optional metric time name, only safe to call when we know that value is set"""
        assert (
            self._metric_time_column_name
        ), "Expected a valid metric time dimension name to be associated with this dataset, but did not get one!"
        return self._metric_time_column_name


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

    @staticmethod
    def make_cumulative_metric_time_range_join_description(
        node: JoinOverTimeRangeNode[SqlDataSetT],
        metric_data_set: AnnotatedSqlDataSet,
        time_spine_data_set: AnnotatedSqlDataSet,
    ) -> SqlJoinDescription:
        """Make a join description to connect a cumulative metric input to a time spine dataset

        Cumulative metrics must be joined against a time spine in a backward-looking fashion, with
        a range determined by a time window (delta against metric_time) and optional cumulative grain.
        """

        # Build an expression like "a.ds <= b.ds AND a.ds >= b.ds - <window>
        # If no window is present we join across all time -> "a.ds <= b.ds"
        metric_time_column_expr = SqlColumnReferenceExpression(
            SqlColumnReference(
                table_alias=metric_data_set.alias,
                column_name=metric_data_set.metric_time_column_name,
            )
        )
        time_spine_column_expr = SqlColumnReferenceExpression(
            SqlColumnReference(
                table_alias=time_spine_data_set.alias,
                column_name=time_spine_data_set.metric_time_column_name,
            )
        )

        # Comparison expression against the endpoint of the cumulative time range,
        # meaning the metric time must always be BEFORE the time spine time
        end_of_range_comparison_expression = SqlComparisonExpression(
            left_expr=metric_time_column_expr,
            comparison=SqlComparison.LESS_THAN_OR_EQUALS,
            right_expr=time_spine_column_expr,
        )

        comparison_expressions: List[SqlComparisonExpression] = [end_of_range_comparison_expression]
        if node.window:
            start_of_range_comparison_expr = SqlComparisonExpression(
                left_expr=metric_time_column_expr,
                comparison=SqlComparison.GREATER_THAN,
                right_expr=SqlTimeDeltaExpression(
                    arg=time_spine_column_expr,
                    count=node.window.count,
                    granularity=node.window.granularity,
                ),
            )
            comparison_expressions.append(start_of_range_comparison_expr)
        elif node.grain_to_date:
            start_of_range_comparison_expr = SqlComparisonExpression(
                left_expr=metric_time_column_expr,
                comparison=SqlComparison.GREATER_THAN_OR_EQUALS,
                right_expr=SqlDateTruncExpression(arg=time_spine_column_expr, time_granularity=node.grain_to_date),
            )
            comparison_expressions.append(start_of_range_comparison_expr)

        cumulative_join_condition = SqlLogicalExpression(
            operator=SqlLogicalOperator.AND,
            args=tuple(comparison_expressions),
        )

        return SqlJoinDescription(
            right_source=metric_data_set.data_set.sql_select_node,
            right_source_alias=metric_data_set.alias,
            on_condition=cumulative_join_condition,
            join_type=SqlJoinType.INNER,
        )
