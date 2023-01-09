from metricflow.sql.render.expr_renderer import (
    DefaultSqlExpressionRenderer,
    SqlExpressionRenderer,
    SqlExpressionRenderResult,
)
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql.sql_exprs import SqlPercentileExpression, SqlGenerateUuidExpression, SqlPercentileFunctionType


class RedshiftSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the Redshift engine."""

    @property
    def double_data_type(self) -> str:
        """Custom double data type for the Redshift engine"""
        return "DOUBLE PRECISION"

    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        """Render a percentile expression for Redshift. Add additional over() syntax for window."""
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        params = arg_rendered.execution_parameters

        if node.percentile_args.function_type == SqlPercentileFunctionType.DISCRETE:
            raise RuntimeError(
                "Redshift SQL Engine does not yet support discrete percentile"
                "aggregation functions. Please disable the use_discrete_percentile flag in all measures."
            )

        function_str = node.percentile_args.function_name
        percentile = node.percentile_args.percentile

        return SqlExpressionRenderResult(
            sql=f"{function_str}({percentile}) WITHIN GROUP (ORDER BY ({arg_rendered.sql}))",
            execution_parameters=params,
        )

    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:  # noqa: D
        """Generates a "good enough" random key to simulate a UUID.

        NOTE: This is a temporary hacky solution as redshift does not have any UUID generation function.

        Proposed solutions that requires more thinking:
            - create a python UDF (Could we insert this without needing additional permissions?)
        """
        return SqlExpressionRenderResult(
            sql="CONCAT(CAST(RANDOM()*100000000 AS INT)::VARCHAR,CAST(RANDOM()*100000000 AS INT)::VARCHAR)",
            execution_parameters=SqlBindParameters(),
        )


class RedshiftSqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for the Redshift engine."""

    EXPR_RENDERER = RedshiftSqlExpressionRenderer()

    @property
    def expr_renderer(self) -> SqlExpressionRenderer:  # noqa :D
        return self.EXPR_RENDERER
