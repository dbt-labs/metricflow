from __future__ import annotations

from typing import Collection

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlArithmeticExpression,
    SqlArithmeticOperator,
    SqlColumnReferenceExpression,
    SqlExtractExpression,
    SqlIntegerExpression,
    SqlPercentileExpression,
    SqlPercentileFunctionType,
    SqlStringExpression,
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


class DorisSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the Apache Doris engine.

    Doris uses MySQL-compatible protocol but has its own SQL dialect differences:
    - Uses DATETIME instead of TIMESTAMP
    - DATE_TRUNC supports both parameter orders; we use the standard ('unit', col) order
    - DAYOFWEEK/EXTRACT(DOW) returns 1=Sunday..7=Saturday (needs ISO conversion)
    - Uses DATE_ADD/DATE_SUB with INTERVAL syntax for date arithmetic
    - Only supports PERCENTILE_APPROX for approximate continuous percentile
    """

    sql_engine = SqlEngine.DORIS

    @override
    def visit_column_reference_expr(self, node: SqlColumnReferenceExpression) -> SqlExpressionRenderResult:
        """Render column references with lowercased table aliases for Doris case sensitivity."""
        if node.should_render_table_alias:
            return SqlExpressionRenderResult(
                sql=f"{node.col_ref.table_alias.lower()}.{node.col_ref.column_name}",
                bind_parameter_set=SqlBindParameterSet(),
            )
        return SqlExpressionRenderResult(
            sql=node.col_ref.column_name,
            bind_parameter_set=SqlBindParameterSet(),
        )

    @override
    def visit_string_expr(self, node: SqlStringExpression) -> SqlExpressionRenderResult:
        """Render string expressions with lowercased table alias references for Doris case sensitivity.

        Doris is case-sensitive for table aliases. When a string expression looks like a simple
        alias.column reference (e.g., 'V.ds'), lowercase the alias part to match the FROM clause.
        """
        sql = node.sql_expr
        if "." in sql and sql.replace(".", "").replace("_", "").isalnum():
            alias, _, rest = sql.partition(".")
            sql = alias.lower() + "." + rest
        return SqlExpressionRenderResult(sql=sql, bind_parameter_set=node.bind_parameter_set)

    @property
    @override
    def double_data_type(self) -> str:
        return "DOUBLE"

    @property
    @override
    def timestamp_data_type(self) -> str:
        """Doris uses DATETIME rather than TIMESTAMP as its primary date-time type."""
        return "DATETIME"

    @property
    @override
    def supported_percentile_function_types(self) -> Collection[SqlPercentileFunctionType]:
        return {SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS}

    @override
    def render_date_part(self, date_part: DatePart) -> str:
        """Render DATE PART for an EXTRACT expression.

        Doris does not support 'isodow'. We use 'DOW' and handle ISO conversion in visit_extract_expr.
        """
        if date_part is DatePart.DOW:
            return "DOW"

        return date_part.value

    @override
    def visit_extract_expr(self, node: SqlExtractExpression) -> SqlExpressionRenderResult:
        """Render EXTRACT with ISO day-of-week conversion for Doris.

        Doris EXTRACT(DOW ...) returns 1=Sunday..7=Saturday, but ISO standard is 1=Monday..7=Sunday.
        We apply: IF(extract_result = 1, 7, extract_result - 1)
        """
        extract_rendering_result = super().visit_extract_expr(node)

        if node.date_part is not DatePart.DOW:
            return extract_rendering_result

        extract_stmt = extract_rendering_result.sql
        case_expr = f"IF({extract_stmt} = 1, 7, {extract_stmt} - 1)"

        return SqlExpressionRenderResult(
            sql=case_expr,
            bind_parameter_set=extract_rendering_result.bind_parameter_set,
        )

    @override
    def visit_subtract_time_interval_expr(self, node: SqlSubtractTimeIntervalExpression) -> SqlExpressionRenderResult:
        """Render time interval subtraction for Doris: DATE_SUB(CAST(col AS DATETIME), INTERVAL count unit)."""
        column = node.arg.accept(self)

        count = node.count
        granularity = node.granularity
        if granularity is TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            count *= 3

        return SqlExpressionRenderResult(
            sql=f"DATE_SUB(CAST({column.sql} AS {self.timestamp_data_type}), INTERVAL {count} {granularity.value})",
            bind_parameter_set=column.bind_parameter_set,
        )

    @override
    def visit_add_time_expr(self, node: SqlAddTimeExpression) -> SqlExpressionRenderResult:
        """Render time addition for Doris: DATE_ADD(CAST(col AS DATETIME), INTERVAL count_expr unit)."""
        granularity = node.granularity
        count_expr = node.count_expr
        if granularity is TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            count_expr = SqlArithmeticExpression.create(
                left_expr=node.count_expr,
                operator=SqlArithmeticOperator.MULTIPLY,
                right_expr=SqlIntegerExpression.create(3),
            )

        column = node.arg.accept(self)
        count = count_expr.accept(self)

        return SqlExpressionRenderResult(
            sql=f"DATE_ADD(CAST({column.sql} AS {self.timestamp_data_type}), INTERVAL {count.sql} {granularity.value})",
            bind_parameter_set=column.bind_parameter_set.merge(count.bind_parameter_set),
        )

    @override
    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        """Render a percentile expression for Doris using PERCENTILE_APPROX."""
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        params = arg_rendered.bind_parameter_set
        percentile = node.percentile_args.percentile

        if node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            return SqlExpressionRenderResult(
                sql=f"PERCENTILE_APPROX({arg_rendered.sql}, {percentile})",
                bind_parameter_set=params,
            )
        elif (
            node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE
            or node.percentile_args.function_type is SqlPercentileFunctionType.CONTINUOUS
            or node.percentile_args.function_type is SqlPercentileFunctionType.DISCRETE
        ):
            raise RuntimeError(
                "Only approximate continuous percentile aggregations are supported for Doris. Set "
                + "use_approximate_percentile and disable use_discrete_percentile in all percentile simple-metrics."
            )
        else:
            assert_values_exhausted(node.percentile_args.function_type)


class DorisSqlPlanRenderer(DefaultSqlPlanRenderer):
    """Plan renderer for the Apache Doris engine."""

    EXPR_RENDERER = DorisSqlExpressionRenderer()

    @property
    @override
    def expr_renderer(self) -> SqlExpressionRenderer:
        return self.EXPR_RENDERER
