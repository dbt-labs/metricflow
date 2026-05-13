from __future__ import annotations

import logging
from typing import Collection

from metricflow_semantics.errors.error_classes import UnsupportedEngineFeatureError
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlBetweenExpression,
    SqlCastToTimestampExpression,
    SqlDateTruncExpression,
    SqlExtractExpression,
    SqlGenerateUuidExpression,
    SqlPercentileExpression,
    SqlPercentileFunctionType,
    SqlSubtractTimeIntervalExpression,
)
from typing_extensions import override

from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.expr_renderer import (
    DefaultSqlExpressionRenderer,
    SqlExpressionRenderer,
    SqlExpressionRenderResult,
)
from metricflow.sql.render.sql_plan_renderer import DefaultSqlPlanRenderer
from metricflow_semantic_interfaces.enum_extension import assert_values_exhausted
from metricflow_semantic_interfaces.type_enums.date_part import DatePart
from metricflow_semantic_interfaces.type_enums.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


class ClickHouseSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the ClickHouse engine.

    ClickHouse has significant differences from standard SQL:
    - Uses toStartOf* functions instead of DATE_TRUNC
    - Parameterized aggregate functions (quantile(0.5)(column))
    - Different data type names (Float64, DateTime64, String)
    - Case-sensitive function names

    Reference: https://clickhouse.com/docs/en/sql-reference/functions
    """

    sql_engine = SqlEngine.CLICKHOUSE

    @property
    @override
    def double_data_type(self) -> str:
        """ClickHouse uses Float64 for double precision floating point."""
        return "Nullable(Float64)"

    @property
    @override
    def timestamp_data_type(self) -> str:
        """ClickHouse uses DateTime for timestamps.

        Note: DateTime64 is available for higher precision, but DateTime
        is the standard type that matches other engines' TIMESTAMP behavior.
        """
        return "Nullable(DateTime64(3))"

    @property
    @override
    def supported_percentile_function_types(self) -> Collection[SqlPercentileFunctionType]:
        """ClickHouse supports multiple percentile function types.

        Reference: https://clickhouse.com/docs/en/sql-reference/aggregate-functions/reference/quantile
        """
        return {
            SqlPercentileFunctionType.CONTINUOUS,
            SqlPercentileFunctionType.DISCRETE,
            SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS,
            SqlPercentileFunctionType.APPROXIMATE_DISCRETE,
        }

    @override
    def visit_date_trunc_expr(self, node: SqlDateTruncExpression) -> SqlExpressionRenderResult:
        """Render DATE_TRUNC for ClickHouse using toStartOf* functions.

        ClickHouse mapping:
        - day -> toStartOfDay
        - week -> toStartOfWeek (requires mode parameter)
        - month -> toStartOfMonth
        - quarter -> toStartOfQuarter
        - year -> toStartOfYear

        Reference: https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions#tostartofday
        """
        self._validate_granularity_for_engine(node.time_granularity)

        arg_rendered = self.render_sql_expr(node.arg)

        # Map TimeGranularity to ClickHouse function
        granularity_map = {
            TimeGranularity.MILLISECOND: "toStartOfMillisecond",
            TimeGranularity.SECOND: "toStartOfSecond",
            TimeGranularity.MINUTE: "toStartOfMinute",
            TimeGranularity.HOUR: "toStartOfHour",
            TimeGranularity.DAY: "toStartOfDay",
            TimeGranularity.WEEK: "toStartOfWeek",  # Mode 1 = ISO week (Monday start)
            TimeGranularity.MONTH: "toStartOfMonth",
            TimeGranularity.QUARTER: "toStartOfQuarter",
            TimeGranularity.YEAR: "toStartOfYear",
        }

        function_name = granularity_map.get(node.time_granularity)
        if not function_name:
            raise UnsupportedEngineFeatureError(
                f"ClickHouse does not support time granularity {node.time_granularity.name}. "
                f"Supported granularities: {list(granularity_map.keys())}"
            )

        # toStartOfWeek requires a mode parameter (1 = ISO week, Monday start)
        if node.time_granularity is TimeGranularity.WEEK:
            sql = f"{function_name}({arg_rendered.sql}, 1)"
        else:
            sql = f"{function_name}({arg_rendered.sql})"

        return SqlExpressionRenderResult(
            sql=sql,
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def render_date_part(self, date_part: DatePart) -> str:
        """Render date part for ClickHouse extract functions.

        ClickHouse uses specific functions instead of EXTRACT:
        - year -> toYear
        - month -> toMonth
        - day -> toDayOfMonth
        - dayofweek -> toDayOfWeek (returns 1-7, Monday=1)
        - dayofyear -> toDayOfYear
        - week -> toISOWeek
        - quarter -> toQuarter

        Reference: https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions#toyear-tomonth
        """
        date_part_map = {
            DatePart.YEAR: "toYear",
            DatePart.MONTH: "toMonth",
            DatePart.DAY: "toDayOfMonth",
            DatePart.DOW: "toDayOfWeek",  # Returns 1-7, Monday=1 (ISO standard)
            DatePart.DOY: "toDayOfYear",
            DatePart.QUARTER: "toQuarter",
        }

        return date_part_map.get(date_part, date_part.value)

    @override
    def visit_extract_expr(self, node: SqlExtractExpression) -> SqlExpressionRenderResult:
        """Render EXTRACT for ClickHouse using to* functions.

        ClickHouse doesn't have EXTRACT, so we use specific functions like
        toYear(), toMonth(), etc.
        """
        arg_rendered = self.render_sql_expr(node.arg)
        date_part_function = self.render_date_part(node.date_part)

        # ClickHouse functions take the date as argument
        sql = f"{date_part_function}({arg_rendered.sql})"

        return SqlExpressionRenderResult(
            sql=sql,
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def visit_add_time_expr(self, node: SqlAddTimeExpression) -> SqlExpressionRenderResult:
        """Render time addition for ClickHouse using add* functions.

        ClickHouse functions:
        - day -> addDays
        - week -> addDays (multiply by 7)
        - month -> addMonths
        - quarter -> addMonths (multiply by 3)
        - year -> addYears

        Reference: https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions#adddays
        """
        arg_rendered = self.render_sql_expr(node.arg)
        count_rendered = self.render_sql_expr(node.count_expr)

        granularity = node.granularity

        # Map granularity to ClickHouse function
        function_map = {
            TimeGranularity.MILLISECOND: "addMilliseconds",
            TimeGranularity.SECOND: "addSeconds",
            TimeGranularity.MINUTE: "addMinutes",
            TimeGranularity.HOUR: "addHours",
            TimeGranularity.DAY: "addDays",
            TimeGranularity.WEEK: "addDays",  # Multiply count by 7
            TimeGranularity.MONTH: "addMonths",
            TimeGranularity.QUARTER: "addMonths",  # Multiply count by 3
            TimeGranularity.YEAR: "addYears",
        }

        function_name = function_map.get(granularity)
        if not function_name:
            raise UnsupportedEngineFeatureError(f"ClickHouse does not support adding {granularity.name} intervals")

        # Handle week and quarter conversions
        if granularity is TimeGranularity.WEEK:
            # Multiply count by 7 and use addDays
            count_sql = f"({count_rendered.sql}) * 7"
            function_name = "addDays"
        elif granularity is TimeGranularity.QUARTER:
            # Multiply count by 3 and use addMonths
            count_sql = f"({count_rendered.sql}) * 3"
            function_name = "addMonths"
        else:
            count_sql = count_rendered.sql if not node.count_expr.requires_parenthesis else f"({count_rendered.sql})"

        sql = f"{function_name}({arg_rendered.sql}, {count_sql})"

        return SqlExpressionRenderResult(
            sql=sql,
            bind_parameter_set=SqlBindParameterSet.merge_iterable(
                (arg_rendered.bind_parameter_set, count_rendered.bind_parameter_set)
            ),
        )

    @override
    def visit_subtract_time_interval_expr(self, node: SqlSubtractTimeIntervalExpression) -> SqlExpressionRenderResult:
        """Render time subtraction for ClickHouse.

        ClickHouse doesn't have subtract functions, so we use negative values
        with add* functions.
        """
        arg_rendered = self.render_sql_expr(node.arg)

        granularity = node.granularity
        count = node.count

        # Map granularity to ClickHouse function
        function_map = {
            TimeGranularity.MILLISECOND: "addMilliseconds",
            TimeGranularity.SECOND: "addSeconds",
            TimeGranularity.MINUTE: "addMinutes",
            TimeGranularity.HOUR: "addHours",
            TimeGranularity.DAY: "addDays",
            TimeGranularity.WEEK: "addDays",
            TimeGranularity.MONTH: "addMonths",
            TimeGranularity.QUARTER: "addMonths",
            TimeGranularity.YEAR: "addYears",
        }

        function_name = function_map.get(granularity)
        if not function_name:
            raise UnsupportedEngineFeatureError(f"ClickHouse does not support subtracting {granularity.name} intervals")

        # Handle week and quarter conversions
        if granularity is TimeGranularity.WEEK:
            count = count * 7
            function_name = "addDays"
        elif granularity is TimeGranularity.QUARTER:
            count = count * 3
            function_name = "addMonths"

        # Use negative count for subtraction
        sql = f"{function_name}({arg_rendered.sql}, -{count})"

        return SqlExpressionRenderResult(
            sql=sql,
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        """Render percentile expression for ClickHouse.

        ClickHouse uses parameterized aggregate functions with curried syntax:
        - quantile(0.5)(column) - approximate continuous
        - quantileExact(0.5)(column) - exact continuous
        - quantileExactLow(0.5)(column) - exact discrete (low)
        - quantileExactHigh(0.5)(column) - exact discrete (high)
        - quantileTiming(0.5)(column) - approximate discrete

        Reference: https://clickhouse.com/docs/en/sql-reference/aggregate-functions/reference/quantile
        """
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        params = arg_rendered.bind_parameter_set
        percentile = node.percentile_args.percentile

        function_type = node.percentile_args.function_type

        # Map MetricFlow percentile types to ClickHouse functions
        if function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            function_str = "quantile"
        elif function_type is SqlPercentileFunctionType.CONTINUOUS:
            function_str = "quantileExact"
        elif function_type is SqlPercentileFunctionType.DISCRETE:
            # ClickHouse doesn't have exact discrete, use low/high
            # Default to low to match typical discrete behavior
            function_str = "quantileExactLow"
        elif function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE:
            function_str = "quantileTiming"
        else:
            assert_values_exhausted(function_type)

        # ClickHouse uses curried function syntax: quantile(percentile)(column)
        sql = f"{function_str}({percentile})({arg_rendered.sql})"

        return SqlExpressionRenderResult(
            sql=sql,
            bind_parameter_set=params,
        )

    @override
    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:
        """Generate UUID for ClickHouse.

        ClickHouse provides generateUUIDv4() function.
        Reference: https://clickhouse.com/docs/en/sql-reference/functions/uuid-functions#generateuuidv4
        """
        return SqlExpressionRenderResult(
            sql="generateUUIDv4()",
            bind_parameter_set=SqlBindParameterSet(),
        )

    @override
    def visit_cast_to_timestamp_expr(self, node: SqlCastToTimestampExpression) -> SqlExpressionRenderResult:
        """Cast to timestamp for ClickHouse.

        ClickHouse uses DateTime64(3) type for timestamps.
        """
        arg_rendered = self.render_sql_expr(node.arg)
        return SqlExpressionRenderResult(
            sql=f"CAST({arg_rendered.sql} AS {self.timestamp_data_type})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def visit_between_expr(self, node: SqlBetweenExpression) -> SqlExpressionRenderResult:
        """Render BETWEEN expression for ClickHouse.

        ClickHouse supports standard BETWEEN syntax.
        For DateTime values, ensure proper casting if needed.
        """
        rendered_column_arg = self.render_sql_expr(node.column_arg)
        rendered_start_expr = self.render_sql_expr(node.start_expr)
        rendered_end_expr = self.render_sql_expr(node.end_expr)

        bind_parameter_set = SqlBindParameterSet()
        bind_parameter_set = bind_parameter_set.merge(rendered_column_arg.bind_parameter_set)
        bind_parameter_set = bind_parameter_set.merge(rendered_start_expr.bind_parameter_set)
        bind_parameter_set = bind_parameter_set.merge(rendered_end_expr.bind_parameter_set)

        sql = f"{rendered_column_arg.sql} BETWEEN {rendered_start_expr.sql} AND {rendered_end_expr.sql}"

        return SqlExpressionRenderResult(
            sql=sql,
            bind_parameter_set=bind_parameter_set,
        )


class ClickHouseSqlPlanRenderer(DefaultSqlPlanRenderer):
    """Plan renderer for the ClickHouse engine.

    Most plan-level rendering follows ANSI SQL, but we may need to override
    specific methods for ClickHouse-specific syntax.
    """

    EXPR_RENDERER = ClickHouseSqlExpressionRenderer()

    @override
    def _render_description_section(self, description: str) -> None:
        """Render the description section as a comment.

        e.g.
        -- Description of the node.

        """
        logger.warning(
            (
                "The ClickHouse SQLAlchemy dialect loses column metadata (result.keys()) "
                "for zero-row results when the SQL query begins with leading -- comment lines. "
                "Comments are suppressed to avoid this bug."
            )
        )

    @property
    @override
    def expr_renderer(self) -> SqlExpressionRenderer:
        return self.EXPR_RENDERER
