from __future__ import annotations

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.sql.render.expr_renderer import (
    DefaultSqlExpressionRenderer,
    SqlExpressionRenderer,
    SqlExpressionRenderResult,
)
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql.sql_exprs import (
    SqlGenerateUuidExpression,
    SqlPercentileExpression,
    SqlPercentileFunctionType,
    SqlTimeDeltaExpression,
)


class DuckDbSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the DuckDB engine."""

    def visit_time_delta_expr(self, node: SqlTimeDeltaExpression) -> SqlExpressionRenderResult:  # noqa: D
        arg_rendered = node.arg.accept(self)
        if node.grain_to_date:
            return SqlExpressionRenderResult(
                sql=f"DATE_TRUNC('{node.granularity.value}', {arg_rendered.sql}::timestamp)",
                bind_parameters=arg_rendered.bind_parameters,
            )

        count = node.count
        granularity = node.granularity
        if granularity == TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            count *= 3

        return SqlExpressionRenderResult(
            sql=f"{arg_rendered.sql} - INTERVAL {count} {granularity.value}",
            bind_parameters=arg_rendered.bind_parameters,
        )

    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:  # noqa: D
        return SqlExpressionRenderResult(
            sql="GEN_RANDOM_UUID()",
            bind_parameters=SqlBindParameters(),
        )

    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        """Render a percentile expression for DuckDB."""
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        params = arg_rendered.bind_parameters
        percentile = node.percentile_args.percentile

        if node.percentile_args.function_type is SqlPercentileFunctionType.CONTINUOUS:
            function_str = "PERCENTILE_CONT"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.DISCRETE:
            function_str = "PERCENTILE_DISC"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            return SqlExpressionRenderResult(
                sql=f"approx_quantile({arg_rendered.sql}, {percentile})",
                bind_parameters=params,
            )
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE:
            raise RuntimeError(
                "Approximate discrete percentile aggregatew not supported for DuckDB. Set "
                + "use_discrete_percentile and/or use_approximate_percentile to false in all percentile measures."
            )
        else:
            assert_values_exhausted(node.percentile_args.function_type)

        return SqlExpressionRenderResult(
            sql=f"{function_str}({percentile}) WITHIN GROUP (ORDER BY ({arg_rendered.sql}))",
            bind_parameters=params,
        )


class DuckDbSqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for the DuckDB engine."""

    EXPR_RENDERER = DuckDbSqlExpressionRenderer()

    @property
    def expr_renderer(self) -> SqlExpressionRenderer:  # noqa :D
        return self.EXPR_RENDERER
