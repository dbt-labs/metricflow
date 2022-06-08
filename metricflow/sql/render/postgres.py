from metricflow.sql.render.expr_renderer import (
    DefaultSqlExpressionRenderer,
    SqlExpressionRenderer,
    SqlExpressionRenderResult,
)
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer
from metricflow.sql.sql_exprs import SqlTimeDeltaExpression
from metricflow.time.time_granularity import TimeGranularity


class PostgresSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the PostgreSQL engine."""

    @property
    def double_data_type(self) -> str:
        """Custom double data type for the PostgreSQL engine"""
        return "DOUBLE PRECISION"

    def visit_time_delta_expr(self, node: SqlTimeDeltaExpression) -> SqlExpressionRenderResult:  # noqa: D
        arg_rendered = node.arg.accept(self)
        if node.grain_to_date:
            return SqlExpressionRenderResult(
                sql=f"DATE_TRUNC('{node.granularity.value}', {arg_rendered.sql}::timestamp)",
                execution_parameters=arg_rendered.execution_parameters,
            )

        count = node.count
        granularity = node.granularity
        if granularity == TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            count *= 3
        return SqlExpressionRenderResult(
            sql=f"{arg_rendered.sql} - MAKE_INTERVAL({granularity.value}s => {count})",
            execution_parameters=arg_rendered.execution_parameters,
        )


class PostgresSQLSqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for the PostgreSQL engine."""

    EXPR_RENDERER = PostgresSqlExpressionRenderer()

    @property
    def expr_renderer(self) -> SqlExpressionRenderer:  # noqa :D
        return self.EXPR_RENDERER
