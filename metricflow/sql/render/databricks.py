from metricflow.object_utils import assert_values_exhausted
from metricflow.sql.render.expr_renderer import (
    DefaultSqlExpressionRenderer,
    SqlExpressionRenderer,
    SqlExpressionRenderResult,
)
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer
from metricflow.sql.sql_exprs import SqlPercentileExpression, SqlPercentileFunctionType


class DatabricksSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the Databricks engine."""

    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        """Render a percentile expression for Databricks. Add additional over() syntax for window."""
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        params = arg_rendered.execution_parameters
        percentile = node.percentile_args.percentile

        if node.percentile_args.function_type is SqlPercentileFunctionType.CONTINUOUS:
            function_str = "PERCENTILE_CONT"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.DISCRETE:
            function_str = "PERCENTILE_DISC"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            raise RuntimeError(
                "Approximate continuous percentile aggregate not supported for Snowflake. Set use_discrete_percentile to true and/or use_approximate_percentile to false in all measures."
            )
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE:
            return SqlExpressionRenderResult(
                sql=f"APPROX_PERCENTILE({arg_rendered.sql}, {percentile})",
                execution_parameters=params,
            )
        else:
            assert_values_exhausted(node.percentile_args.function_type)

        return SqlExpressionRenderResult(
            sql=f"{function_str}({percentile}) WITHIN GROUP (ORDER BY ({arg_rendered.sql}))",
            execution_parameters=params,
        )


class DatabricksSqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for the Snowflake engine."""

    EXPR_RENDERER = DatabricksSqlExpressionRenderer()

    @property
    def expr_renderer(self) -> SqlExpressionRenderer:  # noqa :D
        return self.EXPR_RENDERER
