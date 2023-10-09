from __future__ import annotations

from typing import Collection

from dateutil.parser import parse
from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from typing_extensions import override

from metricflow.sql.render.expr_renderer import (
    DefaultSqlExpressionRenderer,
    SqlExpressionRenderer,
    SqlExpressionRenderResult,
)
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql.sql_exprs import (
    SqlBetweenExpression,
    SqlGenerateUuidExpression,
    SqlPercentileExpression,
    SqlPercentileFunctionType,
    SqlTimeDeltaExpression,
)


class TrinoSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the Trino engine."""

    @property
    @override
    def supported_percentile_function_types(self) -> Collection[SqlPercentileFunctionType]:
        return {
            SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS,
        }

    @override
    def visit_time_delta_expr(self, node: SqlTimeDeltaExpression) -> SqlExpressionRenderResult:
        """Render time delta expression for Trino, which requires slightly different syntax from other engines."""
        arg_rendered = node.arg.accept(self)
        if node.grain_to_date:
            return SqlExpressionRenderResult(
                sql=f"DATE_TRUNC('{node.granularity.value}', TIMESTAMP {arg_rendered.sql})",
                bind_parameters=arg_rendered.bind_parameters,
            )

        count = node.count
        granularity = node.granularity
        if granularity == TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            count *= 3

        # Trino interval needs to be in quotes.
        return SqlExpressionRenderResult(
            sql=f"CAST({arg_rendered.sql} AS {self.timestamp_data_type}) - INTERVAL '{count}' {granularity.value}",
            bind_parameters=arg_rendered.bind_parameters,
        )

    @override
    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:
        return SqlExpressionRenderResult(
            sql="uuid()",
            bind_parameters=SqlBindParameters(),
        )

    @override
    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        """Render a percentile expression for Trino."""
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        params = arg_rendered.bind_parameters
        percentile = node.percentile_args.percentile

        if node.percentile_args.function_type is SqlPercentileFunctionType.CONTINUOUS:
            function_str = "PERCENTILE_CONT"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.DISCRETE:
            function_str = "PERCENTILE_DISC"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            return SqlExpressionRenderResult(
                sql=f"approx_percentile({arg_rendered.sql}, {percentile})",
                bind_parameters=params,
            )
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE:
            raise RuntimeError(
                "Approximate discrete percentile aggregatew not supported for Trino. Set "
                + "use_discrete_percentile and/or use_approximate_percentile to false in all percentile measures."
            )
        else:
            assert_values_exhausted(node.percentile_args.function_type)

        return SqlExpressionRenderResult(
            sql=f"{function_str}({percentile}) WITHIN GROUP (ORDER BY ({arg_rendered.sql}))",
            bind_parameters=params,
        )

    @override
    def visit_between_expr(self, node: SqlBetweenExpression) -> SqlExpressionRenderResult:  # noqa: D
        rendered_column_arg = self.render_sql_expr(node.column_arg)
        rendered_start_expr = self.render_sql_expr(node.start_expr)
        rendered_end_expr = self.render_sql_expr(node.end_expr)

        bind_parameters = SqlBindParameters()
        bind_parameters = bind_parameters.combine(rendered_column_arg.bind_parameters)
        bind_parameters = bind_parameters.combine(rendered_start_expr.bind_parameters)

        # Handle timestamp literals differently.
        if parse(rendered_start_expr.sql):
            sql = f"{rendered_column_arg.sql} BETWEEN timestamp {rendered_start_expr.sql} AND timestamp {rendered_end_expr.sql}"
        else:
            sql = f"{rendered_column_arg.sql} BETWEEN {rendered_start_expr.sql} AND {rendered_end_expr.sql}"

        return SqlExpressionRenderResult(
            sql=sql,
            bind_parameters=bind_parameters,
        )


class TrinoSqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for the Trino engine."""

    EXPR_RENDERER = TrinoSqlExpressionRenderer()

    @property
    @override
    def expr_renderer(self) -> SqlExpressionRenderer:
        return self.EXPR_RENDERER
