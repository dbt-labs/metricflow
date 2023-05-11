from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
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
        """Render a percentile expression for Redshift."""
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        params = arg_rendered.bind_parameters
        percentile = node.percentile_args.percentile

        if node.percentile_args.function_type is SqlPercentileFunctionType.CONTINUOUS:
            function_str = "PERCENTILE_CONT"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.DISCRETE:
            raise RuntimeError(
                "Discrete percentile aggregate not supported for Redshift. Use "
                + "continuous or approximate discrete percentile in all percentile measures."
            )
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            raise RuntimeError(
                "Approximate continuous percentile aggregate not supported for Redshift. Use "
                + "continuous or approximate discrete percentile in all percentile measures."
            )
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE:
            function_str = "APPROXIMATE PERCENTILE_DISC"
        else:
            assert_values_exhausted(node.percentile_args.function_type)

        return SqlExpressionRenderResult(
            sql=f"{function_str}({percentile}) WITHIN GROUP (ORDER BY ({arg_rendered.sql}))",
            bind_parameters=params,
        )

    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:  # noqa: D
        """Generates a "good enough" random key to simulate a UUID.

        NOTE: This is a temporary hacky solution as redshift does not have any UUID generation function.

        Proposed solutions that requires more thinking:
            - create a python UDF (Could we insert this without needing additional permissions?)
        """
        return SqlExpressionRenderResult(
            sql="CONCAT(CAST(RANDOM()*100000000 AS INT)::VARCHAR,CAST(RANDOM()*100000000 AS INT)::VARCHAR)",
            bind_parameters=SqlBindParameters(),
        )


class RedshiftSqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for the Redshift engine."""

    EXPR_RENDERER = RedshiftSqlExpressionRenderer()

    @property
    def expr_renderer(self) -> SqlExpressionRenderer:  # noqa :D
        return self.EXPR_RENDERER
