from dataclasses import dataclass
from typing import List, Optional, Sequence, TypeVar

from metricflow.dataflow.dataflow_plan import JoinOverTimeRangeNode
from metricflow.model.objects.metric import CumulativeMetricWindow
from metricflow.plan_conversion.sql_dataset import SqlDataSet
from metricflow.sql.sql_plan import SqlExpressionNode, SqlJoinDescription, SqlJoinType, SqlSelectStatementNode
from metricflow.sql.sql_exprs import (
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlComparison,
    SqlComparisonExpression,
    SqlLogicalExpression,
    SqlLogicalOperator,
    SqlTimeDeltaExpression,
)
from metricflow.time.time_granularity import TimeGranularity

SqlDataSetT = TypeVar("SqlDataSetT", bound=SqlDataSet)


def _make_cumulative_metric_join_condition(
    metric_data_set_alias: str,
    metric_data_set_metric_time_col: str,
    time_spine_data_set_alias: str,
    time_spine_data_set_time_col: str,
    window: Optional[CumulativeMetricWindow],
    grain_to_date: Optional[TimeGranularity],
) -> SqlExpressionNode:
    """Creates SqlExpression representing a cumulative metric self-join conditions"""

    # Comparison expression against the endpoint of the cumulative time range,
    # meaning the metric time must always be BEFORE the time spine time
    end_of_range_comparison_expression = SqlComparisonExpression(
        left_expr=SqlColumnReferenceExpression(
            SqlColumnReference(
                table_alias=metric_data_set_alias,
                column_name=metric_data_set_metric_time_col,
            )
        ),
        comparison=SqlComparison.LESS_THAN_OR_EQUALS,
        right_expr=SqlColumnReferenceExpression(
            SqlColumnReference(
                table_alias=time_spine_data_set_alias,
                column_name=time_spine_data_set_time_col,
            )
        ),
    )

    if window is None and grain_to_date is None:  # accumulate over all time
        return end_of_range_comparison_expression

    # Set up configuration for the start of range comparison expression. The operator
    # and time delta window/granularity differ depending on whether it's a window-based
    # or grain-to-date accumulation
    if window is not None and grain_to_date is None:
        start_of_range_comparison_operator = SqlComparison.GREATER_THAN
        delta_count = window.count
        delta_granularity = window.granularity
    elif window is None and grain_to_date is not None:
        start_of_range_comparison_operator = SqlComparison.GREATER_THAN_OR_EQUALS
        delta_count = 0
        delta_granularity = grain_to_date
    else:
        raise ValueError(
            f"Attempting to build a time spine join with both a window and a grain_to_date set! Window: {window}. "
            f"Grain_to_date: {grain_to_date}."
        )

    # ds <= <date> and  ds > <date> - <window>
    return SqlLogicalExpression(
        operator=SqlLogicalOperator.AND,
        args=(
            SqlComparisonExpression(
                left_expr=SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=metric_data_set_alias,
                        column_name=metric_data_set_metric_time_col,
                    )
                ),
                comparison=SqlComparison.LESS_THAN_OR_EQUALS,
                right_expr=SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=time_spine_data_set_alias,
                        column_name=time_spine_data_set_time_col,
                    )
                ),
            ),
            SqlComparisonExpression(
                left_expr=SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=metric_data_set_alias,
                        column_name=metric_data_set_metric_time_col,
                    )
                ),
                comparison=start_of_range_comparison_operator,
                right_expr=SqlTimeDeltaExpression(
                    SqlColumnReferenceExpression(
                        SqlColumnReference(
                            table_alias=time_spine_data_set_alias,
                            column_name=time_spine_data_set_time_col,
                        )
                    ),
                    count=delta_count,
                    granularity=delta_granularity,
                    grain_to_date=grain_to_date,
                ),
            ),
        ),
    )


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
        constrain_metric_time_column_condition_both = _make_cumulative_metric_join_condition(
            metric_data_set.alias,
            metric_data_set.metric_time_column_name,
            time_spine_data_set.alias,
            time_spine_data_set.metric_time_column_name,
            node.window,
            node.grain_to_date,
        )

        return SqlJoinDescription(
            right_source=metric_data_set.data_set.sql_select_node,
            right_source_alias=metric_data_set.alias,
            on_condition=constrain_metric_time_column_condition_both,
            join_type=SqlJoinType.INNER,
        )
