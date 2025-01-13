from __future__ import annotations

import textwrap
from typing import Collection, Optional, Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.sql.render.rendering_constants import SqlRenderingConstants
from metricflow.sql.sql_plan import SqlJoinDescription
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
    SqlSubtractTimeIntervalExpression, SqlExpressionNode,
)
from typing_extensions import override

from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.expr_renderer import (
    DefaultSqlExpressionRenderer,
    SqlExpressionRenderer,
    SqlExpressionRenderResult,
)
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer, SqlPlanRenderResult


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

    @override
    def visit_subtract_time_interval_expr(self, node: SqlSubtractTimeIntervalExpression) -> SqlExpressionRenderResult:
        """Render time delta operations for Clickhouse, which needs custom support for quarterly granularity."""
        arg_rendered = node.arg.accept(self)

        count = node.count
        granularity = node.granularity
        if granularity is TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            count *= self.__QUARTER_IN_MONTHS

        function_name = self.__get_function_operation_from_time_granularity(granularity)

        return SqlExpressionRenderResult(
            sql=f"{function_name}({arg_rendered.sql}, CAST(-{count} AS Integer))",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def visit_add_time_expr(self, node: SqlAddTimeExpression) -> SqlExpressionRenderResult:
        """Render time delta operations for Clickhouse, which needs custom support for quarterly granularity."""
        granularity = node.granularity
        count_expr = node.count_expr
        if granularity is TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            SqlArithmeticExpression.create(
                left_expr=node.count_expr,
                operator=SqlArithmeticOperator.MULTIPLY,
                right_expr=SqlIntegerExpression.create(self.__QUARTER_IN_MONTHS),
            )  # TODO: this is not correct, we need to multiply the count by the number of months in a quarter ?

        arg_rendered = node.arg.accept(self)
        count_rendered = count_expr.accept(self)
        count_sql = f"({count_rendered.sql})" if count_expr.requires_parenthesis else count_rendered.sql

        function_operation = self.__get_function_operation_from_time_granularity(granularity)

        return SqlExpressionRenderResult(
            sql=f"{function_operation}({arg_rendered.sql}, CAST({count_sql} AS Integer))",
            bind_parameter_set=SqlBindParameterSet.merge_iterable(
                (arg_rendered.bind_parameter_set, count_rendered.bind_parameter_set)
            ),
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


class ClickhouseSqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for the Clickhouse engine."""

    EXPR_RENDERER = ClickhouseSqlExpressionRenderer()

    @property
    @override
    def expr_renderer(self) -> SqlExpressionRenderer:
        return self.EXPR_RENDERER

    @override
    def _render_adapter_specific_flags(self) -> Optional[SqlPlanRenderResult]:
        """Add ClickHouse-specific query settings."""
        settings = [
            "allow_experimental_join_condition = 1",
            "allow_experimental_analyzer = 1",
            "join_use_nulls = 0"
        ]
        return SqlPlanRenderResult(
            sql=f"SETTINGS {', '.join(settings)}",
            bind_parameter_set=SqlBindParameterSet()
        )
    def _render_joins_section(self, join_descriptions: Sequence[SqlJoinDescription]) -> Optional[SqlPlanRenderResult]:
        """Convert the join descriptions into a "JOIN" section with ClickHouse-specific handling."""
        if len(join_descriptions) == 0:
            return None

        params = SqlBindParameterSet()
        join_section_lines = []
        where_conditions = []

        for join_description in join_descriptions:
            right_source_rendered = self._render_node(join_description.right_source)
            params = params.merge(right_source_rendered.bind_parameter_set)

            on_condition_rendered: Optional[SqlExpressionRenderResult] = None
            if join_description.on_condition:
                on_condition_rendered = self.EXPR_RENDERER.render_sql_expr(join_description.on_condition)
                params = params.merge(on_condition_rendered.bind_parameter_set)

            # Check if this is a time-range join
            is_time_range_join = False
            if on_condition_rendered:
                is_time_range_join = any(op in on_condition_rendered.sql for op in ['<=', '>=', '<', '>'])

            # Add join type
            join_section_lines.append(join_description.join_type.value)

            # Add the source
            if join_description.right_source.as_sql_table_node is not None:
                join_section_lines.append(
                    textwrap.indent(
                        f"{right_source_rendered.sql} {join_description.right_source_alias}",
                        prefix=SqlRenderingConstants.INDENT,
                    )
                )
            else:
                join_section_lines.append("(")
                join_section_lines.append(
                    textwrap.indent(right_source_rendered.sql, prefix=SqlRenderingConstants.INDENT)
                )
                join_section_lines.append(f") {join_description.right_source_alias}")

            # Add conditions
            if is_time_range_join:
                # For time-range joins, convert to CROSS JOIN + WHERE
                join_section_lines[-len(join_section_lines)] = "CROSS JOIN"  # Replace join type
                if on_condition_rendered:
                    where_conditions.append(on_condition_rendered.sql)
            else:
                # For regular joins, use ON clause
                if on_condition_rendered:
                    join_section_lines.append("ON")
                    join_section_lines.append(
                        textwrap.indent(on_condition_rendered.sql, prefix=SqlRenderingConstants.INDENT)
                    )

        # Store where conditions for use in _render_where_section
        if where_conditions:
            self._stored_where_conditions = where_conditions

        return SqlPlanRenderResult("\n".join(join_section_lines), params)

@override
def _render_where(self, where_expression: Optional[SqlExpressionNode]) -> Optional[SqlPlanRenderResult]:
    """Override to combine stored where conditions from joins with the main where clause."""
    original_where = super()._render_where(where_expression)
    stored_conditions = getattr(self, '_stored_where_conditions', [])

    if not stored_conditions and not original_where:
        return None

    conditions = []

    # Add original where condition if it exists
    if original_where:
        # Strip the "WHERE" prefix if it exists
        where_sql = original_where.sql
        if where_sql.upper().startswith('WHERE '):
            where_sql = where_sql[6:]
        conditions.append(where_sql)

    # Add stored conditions from joins
    conditions.extend(stored_conditions)

    # Combine all conditions with AND
    combined_sql = ' AND '.join(f'({condition})' for condition in conditions if condition.strip())

    return SqlPlanRenderResult(
        sql=f"WHERE {combined_sql}",
        bind_parameter_set=original_where.bind_parameter_set if original_where else SqlBindParameterSet(),
    )