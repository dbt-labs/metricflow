from __future__ import annotations

import re
import textwrap
from typing import Collection, Optional, Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.errors.error_classes import UnsupportedEngineFeatureError
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlArithmeticExpression,
    SqlArithmeticOperator,
    SqlGenerateUuidExpression,
    SqlIntegerExpression,
    SqlPercentileExpression,
    SqlPercentileFunctionType,
    SqlSubtractTimeIntervalExpression, SqlExpressionNode, SqlExtractExpression, SqlDateTruncExpression,
    SqlCastToTimestampExpression,
)
from typing_extensions import override

from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.expr_renderer import (
    DefaultSqlExpressionRenderer,
    SqlExpressionRenderer,
    SqlExpressionRenderResult,
)
from metricflow.sql.render.rendering_constants import SqlRenderingConstants
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer, SqlPlanRenderResult
from metricflow.sql.sql_plan import SqlJoinDescription, SqlSelectColumn
from metricflow_semantics.sql.sql_join_type import SqlJoinType


class ClickhouseSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the Clickhouse engine."""

    __QUARTER_IN_MONTHS = 3

    sql_engine = SqlEngine.CLICKHOUSE

    @property
    @override
    def double_data_type(self) -> str:
        """Custom double data type for the Clickhouse engine."""
        return "DOUBLE PRECISION"

    @property
    @override
    def supported_percentile_function_types(self) -> Collection[SqlPercentileFunctionType]:
        return {
            SqlPercentileFunctionType.CONTINUOUS,
            SqlPercentileFunctionType.DISCRETE,
            SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS,
        }

    @override
    def render_date_part(self, date_part: DatePart) -> str:
        """Map DatePart enum to Clickhouse date/time function names."""
        if date_part is DatePart.DOW:
            return "toDayOfWeek"  # Returns 1-7 where Monday is 1
        elif date_part is DatePart.DOY:
            return "toDayOfYear"
        elif date_part is DatePart.MONTH:
            return "toMonth"
        elif date_part is DatePart.QUARTER:
            return "toQuarter"
        elif date_part is DatePart.YEAR:
            return "toYear"
        elif date_part is DatePart.DAY:
            return "toDayOfMonth"
        return assert_values_exhausted(date_part)

    # @override
    # def visit_subtract_time_interval_expr(self, node: SqlSubtractTimeIntervalExpression) -> SqlExpressionRenderResult:
    #     """Render time delta operations for Clickhouse, which needs custom support for quarterly granularity."""
    #     arg_rendered = node.arg.accept(self)
    #
    #     count = node.count
    #     granularity = node.granularity
    #     if granularity is TimeGranularity.QUARTER:
    #         granularity = TimeGranularity.MONTH
    #         count *= self.__QUARTER_IN_MONTHS
    #
    #     function_name = self.__get_function_operation_from_time_granularity(granularity)
    #
    #     return SqlExpressionRenderResult(
    #         sql=f"{function_name}({arg_rendered.sql}, CAST(-{count} AS Integer))",
    #         bind_parameter_set=arg_rendered.bind_parameter_set,
    #     )
    #
    # @override
    # def visit_add_time_expr(self, node: SqlAddTimeExpression) -> SqlExpressionRenderResult:
    #     """Render time delta operations for Clickhouse, which needs custom support for quarterly granularity."""
    #     granularity = node.granularity
    #     count_expr = node.count_expr
    #     if granularity is TimeGranularity.QUARTER:
    #         granularity = TimeGranularity.MONTH
    #         SqlArithmeticExpression.create(
    #             left_expr=node.count_expr,
    #             operator=SqlArithmeticOperator.MULTIPLY,
    #             right_expr=SqlIntegerExpression.create(self.__QUARTER_IN_MONTHS),
    #         )  # TODO: this is not correct, we need to multiply the count by the number of months in a quarter ?
    #
    #     arg_rendered = node.arg.accept(self)
    #     count_rendered = count_expr.accept(self)
    #     count_sql = f"({count_rendered.sql})" if count_expr.requires_parenthesis else count_rendered.sql
    #
    #     function_operation = self.__get_function_operation_from_time_granularity(granularity)
    #
    #     return SqlExpressionRenderResult(
    #         sql=f"{function_operation}({arg_rendered.sql}, CAST({count_sql} AS Integer))",
    #         bind_parameter_set=SqlBindParameterSet.merge_iterable(
    #             (arg_rendered.bind_parameter_set, count_rendered.bind_parameter_set)
    #         ),
    #     )
    #
    @override
    def visit_cast_to_timestamp_expr(self, node: SqlCastToTimestampExpression) -> SqlExpressionRenderResult:
        """Override to use Clickhouse's timestamp casting."""
        arg_rendered = self.render_sql_expr(node.arg)

        # For timestamp casting, use toDateTime
        return SqlExpressionRenderResult(
            sql=f"toDateTime({arg_rendered.sql})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def visit_date_trunc_expr(self, node: SqlDateTruncExpression) -> SqlExpressionRenderResult:
        self._validate_granularity_for_engine(node.time_granularity)

        """Handle date truncation expressions properly."""
        arg_rendered = self.render_sql_expr(node.arg)

        # Map the granularity to Clickhouse's date truncation functions
        trunc_function = {
            TimeGranularity.DAY: "day",
            TimeGranularity.WEEK: "week",
            TimeGranularity.MONTH: "month",
            TimeGranularity.QUARTER: "quarter",
            TimeGranularity.YEAR: "year",
            TimeGranularity.HOUR: "hour",
            TimeGranularity.MILLISECOND: "milisecond",
            TimeGranularity.SECOND: "second",
            TimeGranularity.MINUTE: "minute"
        }[node.time_granularity]

        return SqlExpressionRenderResult(
            sql=f"date_trunc('{trunc_function}', {arg_rendered.sql})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:
        return SqlExpressionRenderResult(
            sql="generateUUIDv4()",
            bind_parameter_set=SqlBindParameterSet(),
        )

    @override
    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        """Render a percentile expression for Clickhouse."""
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        params = arg_rendered.bind_parameter_set
        percentile = node.percentile_args.percentile

        if node.percentile_args.function_type is SqlPercentileFunctionType.CONTINUOUS:
            function_str = "quantile"  # Uses interpolation by default
        elif node.percentile_args.function_type is SqlPercentileFunctionType.DISCRETE:
            function_str = "quantileExact"  # Exact calculation without interpolation
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            function_str = "quantile"  # Default quantile is already approximate
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE:
            raise UnsupportedEngineFeatureError(
                "Approximate discrete percentile aggregate not supported for Clickhouse. Set "
                + "use_approximate_percentile to false in all percentile measures."
            )
        else:
            assert_values_exhausted(node.percentile_args.function_type)

        # Clickhouse uses function(percentile)(expr) syntax instead of WITHIN GROUP
        return SqlExpressionRenderResult(
            sql=f"{function_str}({percentile})({arg_rendered.sql})",
            bind_parameter_set=params,
        )

    @override
    def visit_extract_expr(self, node: SqlExtractExpression) -> SqlExpressionRenderResult:
        """Renders extract expressions with required output conversions for Clickhouse.

        Clickhouse doesn't support extract for dates. It has its own functions to extract data from timestamps.
        """

        arg_rendered = self.render_sql_expr(node.arg)

        return SqlExpressionRenderResult(
            sql=f"{self.render_date_part(node.date_part)}({arg_rendered.sql})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    def __get_function_operation_from_time_granularity(self, granularity: TimeGranularity) -> str:
        return {
            TimeGranularity.YEAR: "addYears",
            TimeGranularity.QUARTER: "addMonths",
            TimeGranularity.MONTH: "addMonths",
            TimeGranularity.WEEK: "addWeeks",
            TimeGranularity.DAY: "addDays",
            TimeGranularity.HOUR: "addHours",
            TimeGranularity.MINUTE: "addMinutes",
            TimeGranularity.SECOND: "addSeconds",
        }[granularity]

    @override
    def render_group_by_expr(self, group_by_column: SqlSelectColumn) -> SqlExpressionRenderResult:
        """Custom rendering of group by column expressions.

        BigQuery requires group bys to be referenced by alias, rather than duplicating the expression from the SELECT

        e.g.,
          SELECT COALESCE(x, y) AS x_or_y, SUM(1)
          FROM source_table
          GROUP BY x_or_y

        By default we would render GROUP BY COALESCE(x, y) on that last line, and BigQuery will throw an exception
        """
        return SqlExpressionRenderResult(
            sql=group_by_column.column_alias,
            bind_parameter_set=group_by_column.expr.bind_parameter_set,
        )


class ClickhouseSqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for the Clickhouse engine."""

    EXPR_RENDERER = ClickhouseSqlExpressionRenderer()

    @property
    @override
    def expr_renderer(self) -> SqlExpressionRenderer:
        return self.EXPR_RENDERER



    #
    # @override
    # def _render_where(self, where_expression: Optional[SqlExpressionNode]) -> Optional[SqlPlanRenderResult]:
    #     """Override to combine stored where conditions from joins with the main where clause."""
    #     original_where = super()._render_where(where_expression)
    #     stored_conditions = getattr(self, '_stored_where_conditions', [])
    #
    #     # Clear stored conditions to prevent them from being used multiple times
    #     self._stored_where_conditions = []
    #
    #     if not stored_conditions and not original_where:
    #         return None
    #
    #     conditions = []
    #
    #     # Add original where condition if it exists
    #     if original_where:
    #         # Strip the "WHERE" prefix if it exists
    #         where_sql = original_where.sql
    #         if where_sql.upper().startswith('WHERE '):
    #             where_sql = where_sql[6:]
    #         conditions.append(where_sql)
    #
    #     # Add stored conditions from joins (only unique conditions)
    #     seen_conditions = set()
    #     for condition in stored_conditions:
    #         if condition not in seen_conditions:
    #             conditions.append(condition)
    #             seen_conditions.add(condition)
    #
    #     # Combine all conditions with AND
    #     combined_sql = ' AND '.join(f'({condition})' for condition in conditions if condition.strip())
    #
    #     return SqlPlanRenderResult(
    #         sql=f"WHERE {combined_sql}" if combined_sql else "",
    #         bind_parameter_set=original_where.bind_parameter_set if original_where else SqlBindParameterSet(),
    #     )
    #
    #
    # @override
    # def _render_joins_section(self, join_descriptions: Sequence[SqlJoinDescription]) -> Optional[SqlPlanRenderResult]:
    #     """Convert the join descriptions into a "JOIN" section with ClickHouse-specific handling.
    #
    #     Args:
    #         join_descriptions: Sequence of join descriptions to render
    #
    #     Returns:
    #         SqlPlanRenderResult with the rendered JOIN section, or None if no joins
    #     """
    #     if not join_descriptions:
    #         return None
    #
    #     params = SqlBindParameterSet()
    #     join_section_lines = []
    #     where_conditions = []
    #
    #     for join_description in join_descriptions:
    #         join_lines, join_params, join_where_conditions = self._render_single_join(join_description)
    #         join_section_lines.extend(join_lines)
    #         params = params.merge(join_params)
    #         where_conditions.extend(join_where_conditions)
    #
    #     # Store where conditions for use in _render_where_section
    #     if where_conditions:
    #         self._stored_where_conditions = where_conditions
    #
    #     return SqlPlanRenderResult("\n".join(join_section_lines), params)
    #
    # def _render_single_join(self, join_description: SqlJoinDescription) -> tuple[list[str], SqlBindParameterSet, list[str]]:
    #     """Renders a single join description."""
    #     join_lines = []
    #     where_conditions = []
    #     params = SqlBindParameterSet()
    #
    #     # Render the right source
    #     right_source_rendered = self._render_node(join_description.right_source)
    #     params = params.merge(right_source_rendered.bind_parameter_set)
    #
    #     # Render the ON condition if present
    #     on_condition = self._render_join_condition(join_description.on_condition)
    #     if on_condition:
    #         on_condition = SqlExpressionRenderResult(
    #             sql=on_condition.sql,
    #             bind_parameter_set=on_condition.bind_parameter_set
    #         )
    #         params = params.merge(on_condition.bind_parameter_set)
    #
    #     # Add join type and source
    #     join_lines.append(join_description.join_type.value)
    #     source_lines = self.__render_join_source(
    #         right_source_rendered.sql,
    #         join_description.right_source_alias,
    #         join_description.right_source.as_sql_table_node is not None
    #     )
    #     join_lines.extend(source_lines)
    #
    #     # Always add ON clause for Clickhouse, even for inequality joins
    #     if on_condition:
    #         if self.__is_inequality_join(on_condition.sql):
    #             # For inequality joins, use a simple 1=1 in ON clause and move real condition to WHERE
    #             join_lines.extend(["ON", textwrap.indent("1 = 1", prefix=SqlRenderingConstants.INDENT)])
    #             where_conditions.append(on_condition.sql)
    #         else:
    #             join_lines.extend(["ON", textwrap.indent(on_condition.sql, prefix=SqlRenderingConstants.INDENT)])
    #
    #     return join_lines, params, where_conditions
    #
    # def _render_join_condition(self, condition: Optional[SqlExpressionNode]) -> Optional[SqlExpressionRenderResult]:
    #     """Renders the JOIN's ON condition if present."""
    #     if condition:
    #         return self.EXPR_RENDERER.render_sql_expr(condition)
    #     return None
    #
    # def __get_join_type(self, original_type: SqlJoinType, on_condition: Optional[SqlExpressionRenderResult]) -> SqlJoinType:
    #     """Determines the final join type, converting to CROSS JOIN for time-range joins."""
    #     if on_condition and self.__is_inequality_join(on_condition.sql):
    #         return SqlJoinType.CROSS_JOIN
    #     return original_type
    #
    # @staticmethod
    # def __is_inequality_join(condition_sql: str) -> bool:
    #     """Checks if a join condition contains time range operators."""
    #     inequality_operators = ["<=", ">=", "<", ">"]
    #     return any(op in condition_sql for op in inequality_operators)
    #
    # @staticmethod
    # def __render_join_source(source_sql: str, alias: str, is_table: bool) -> list[str]:
    #     """Renders the join's source table or subquery with proper indentation."""
    #     if is_table:
    #         return [textwrap.indent(f"{source_sql} AS {alias.lower()}", prefix=SqlRenderingConstants.INDENT)]
    #
    #     return [
    #         "(",
    #         textwrap.indent(source_sql, prefix=SqlRenderingConstants.INDENT),
    #         f") AS {alias.lower()}"  # Force lowercase here too
    #     ]