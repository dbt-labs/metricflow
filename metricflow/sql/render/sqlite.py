from metricflow.sql.render.expr_renderer import (
    DefaultSqlExpressionRenderer,
    SqlExpressionRenderer,
    SqlExpressionRenderResult,
    SqlTimeDeltaExpression,
)
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer
from metricflow.sql.sql_exprs import SqlCastToTimestampExpression, SqlDateTruncExpression


class SqliteSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the SQLite engine."""

    def visit_cast_to_timestamp_expr(self, node: SqlCastToTimestampExpression) -> SqlExpressionRenderResult:  # noqa: D
        # SQLite doesn't support timestamps, so all datetime related values are treated as strings.
        arg_rendered = self.render_sql_expr(node.arg)
        return SqlExpressionRenderResult(
            sql=f"CAST({arg_rendered.sql} AS TEXT)",
            execution_parameters=arg_rendered.execution_parameters,
        )

    def visit_time_delta_expr(self, node: SqlTimeDeltaExpression) -> SqlExpressionRenderResult:  # noqa: D
        arg_rendered = node.arg.accept(self)
        if node.grain_to_date:
            return SqlExpressionRenderResult(
                sql=f"DATE({arg_rendered.sql}, 'start of {node.granularity.value}')",
                execution_parameters=arg_rendered.execution_parameters,
            )

        return SqlExpressionRenderResult(
            sql=f"DATE({arg_rendered.sql}, '-{node.count} {node.granularity.value}')",
            execution_parameters=arg_rendered.execution_parameters,
        )

    def visit_date_trunc_expr(self, node: SqlDateTruncExpression) -> SqlExpressionRenderResult:  # noqa: D
        arg_rendered = self.render_sql_expr(node.arg)

        # SQLite doesn't have the method, but it would be helpful to show something in the plan outputs.
        return SqlExpressionRenderResult(
            sql="'__DATE_TRUNC_NOT_SUPPORTED__'",
            execution_parameters=arg_rendered.execution_parameters,
        )


class SqliteSqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for the SQLite engine."""

    EXPR_RENDERER = SqliteSqlExpressionRenderer()

    @property
    def expr_renderer(self) -> SqlExpressionRenderer:  # noqa :D
        return self.EXPR_RENDERER
