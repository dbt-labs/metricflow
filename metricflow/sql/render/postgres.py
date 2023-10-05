from __future__ import annotations

from typing import Collection

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from typing_extensions import override

from metricflow.errors.errors import UnsupportedEngineFeatureError
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
    SqlSubtractTimeIntervalExpression,
)


class PostgresSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the PostgreSQL engine."""

    @property
    @override
    def double_data_type(self) -> str:
        """Custom double data type for the PostgreSQL engine."""
        return "DOUBLE PRECISION"

    @property
    @override
    def supported_percentile_function_types(self) -> Collection[SqlPercentileFunctionType]:
        return {SqlPercentileFunctionType.CONTINUOUS, SqlPercentileFunctionType.DISCRETE}

    @override
    def visit_time_delta_expr(self, node: SqlSubtractTimeIntervalExpression) -> SqlExpressionRenderResult:
        """Render time delta operations for PostgreSQL, which needs custom support for quarterly granularity."""
        arg_rendered = node.arg.accept(self)

        count = node.count
        granularity = node.granularity
        if granularity == TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            count *= 3
        return SqlExpressionRenderResult(
            sql=f"{arg_rendered.sql} - MAKE_INTERVAL({granularity.value}s => {count})",
            bind_parameters=arg_rendered.bind_parameters,
        )

    @override
    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:
        return SqlExpressionRenderResult(
            sql="GEN_RANDOM_UUID()",
            bind_parameters=SqlBindParameters(),
        )

    @override
    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        """Render a percentile expression for Postgres."""
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        params = arg_rendered.bind_parameters
        percentile = node.percentile_args.percentile

        if node.percentile_args.function_type is SqlPercentileFunctionType.CONTINUOUS:
            function_str = "PERCENTILE_CONT"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.DISCRETE:
            function_str = "PERCENTILE_DISC"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            raise UnsupportedEngineFeatureError(
                "Approximate continuous percentile aggregate not supported for Postgres. Set "
                + "use_approximate_percentile to false in all percentile measures."
            )
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE:
            raise UnsupportedEngineFeatureError(
                "Approximate discrete percentile aggregate not supported for Postgres. Set "
                + "use_approximate_percentile to false in all percentile measures."
            )
        else:
            assert_values_exhausted(node.percentile_args.function_type)

        return SqlExpressionRenderResult(
            sql=f"{function_str}({percentile}) WITHIN GROUP (ORDER BY ({arg_rendered.sql}))",
            bind_parameters=params,
        )


class PostgresSQLSqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for the PostgreSQL engine."""

    EXPR_RENDERER = PostgresSqlExpressionRenderer()

    @property
    @override
    def expr_renderer(self) -> SqlExpressionRenderer:
        return self.EXPR_RENDERER
