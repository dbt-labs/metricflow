from __future__ import annotations

import logging
from typing import Collection

from metricflow_semantics.errors.error_classes import UnsupportedEngineFeatureError
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlArithmeticExpression,
    SqlArithmeticOperator,
    SqlDateTruncExpression,
    SqlExtractExpression,
    SqlGenerateUuidExpression,
    SqlIntegerExpression,
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


def clickhouse_dry_run_statement(stmt: str) -> str:
    """Return a ClickHouse statement that validates SQL without executing the original statement."""
    normalized_stmt = stmt.lstrip().upper()
    explain_prefix = "EXPLAIN" if normalized_stmt.startswith(("SELECT", "WITH")) else "EXPLAIN AST"
    return f"{explain_prefix} {stmt}"


class ClickHouseSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the ClickHouse engine."""

    sql_engine = SqlEngine.CLICKHOUSE

    @property
    @override
    def double_data_type(self) -> str:
        return "Float64"

    @property
    @override
    def timestamp_data_type(self) -> str:
        return "DateTime64(6)"

    @property
    @override
    def supported_percentile_function_types(self) -> Collection[SqlPercentileFunctionType]:
        return {
            SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS,
        }

    @override
    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:
        return SqlExpressionRenderResult(
            sql="generateUUIDv4()",
            bind_parameter_set=SqlBindParameterSet(),
        )

    @override
    def visit_date_trunc_expr(self, node: SqlDateTruncExpression) -> SqlExpressionRenderResult:
        self._validate_granularity_for_engine(node.time_granularity)

        arg_rendered = self.render_sql_expr(node.arg)
        return SqlExpressionRenderResult(
            sql=f"dateTrunc('{node.time_granularity.value}', {arg_rendered.sql})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def visit_extract_expr(self, node: SqlExtractExpression) -> SqlExpressionRenderResult:
        arg_rendered = self.render_sql_expr(node.arg)

        if node.date_part is DatePart.DOW:
            return SqlExpressionRenderResult(
                sql=f"toDayOfWeek({arg_rendered.sql})",
                bind_parameter_set=arg_rendered.bind_parameter_set,
            )
        elif node.date_part is DatePart.DOY:
            return SqlExpressionRenderResult(
                sql=f"toDayOfYear({arg_rendered.sql})",
                bind_parameter_set=arg_rendered.bind_parameter_set,
            )

        return SqlExpressionRenderResult(
            sql=f"EXTRACT({self.render_date_part(node.date_part)} FROM {arg_rendered.sql})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def render_date_part(self, date_part: DatePart) -> str:
        return date_part.value

    @override
    def visit_subtract_time_interval_expr(self, node: SqlSubtractTimeIntervalExpression) -> SqlExpressionRenderResult:
        arg_rendered = node.arg.accept(self)

        count = node.count
        granularity = node.granularity
        if granularity is TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            count *= 3

        return SqlExpressionRenderResult(
            sql=f"dateAdd('{granularity.value}', -{count}, {arg_rendered.sql})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def visit_add_time_expr(self, node: SqlAddTimeExpression) -> SqlExpressionRenderResult:
        granularity = node.granularity
        count_expr = node.count_expr
        if granularity is TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            count_expr = SqlArithmeticExpression.create(
                left_expr=node.count_expr,
                operator=SqlArithmeticOperator.MULTIPLY,
                right_expr=SqlIntegerExpression.create(3),
            )

        arg_rendered = node.arg.accept(self)
        count_rendered = count_expr.accept(self)
        count_sql = f"({count_rendered.sql})" if count_expr.requires_parenthesis else count_rendered.sql

        return SqlExpressionRenderResult(
            sql=f"dateAdd('{granularity.value}', {count_sql}, {arg_rendered.sql})",
            bind_parameter_set=SqlBindParameterSet.merge_iterable(
                (arg_rendered.bind_parameter_set, count_rendered.bind_parameter_set)
            ),
        )

    @override
    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        percentile = node.percentile_args.percentile

        if node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            return SqlExpressionRenderResult(
                sql=f"quantile({percentile})({arg_rendered.sql})",
                bind_parameter_set=arg_rendered.bind_parameter_set,
            )
        elif (
            node.percentile_args.function_type is SqlPercentileFunctionType.CONTINUOUS
            or node.percentile_args.function_type is SqlPercentileFunctionType.DISCRETE
            or node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE
        ):
            raise UnsupportedEngineFeatureError(
                "Only approximate continuous percentile aggregations are supported for ClickHouse. Set "
                + "use_approximate_percentile to true and disable use_discrete_percentile in all percentile "
                + "simple-metrics."
            )
        else:
            assert_values_exhausted(node.percentile_args.function_type)


class ClickHouseSqlPlanRenderer(DefaultSqlPlanRenderer):
    """Plan renderer for the ClickHouse engine."""

    EXPR_RENDERER = ClickHouseSqlExpressionRenderer()

    @property
    @override
    def expr_renderer(self) -> SqlExpressionRenderer:
        return self.EXPR_RENDERER
