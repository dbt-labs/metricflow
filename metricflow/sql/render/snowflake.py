from __future__ import annotations

from typing import Collection

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from metricflow_semantics.errors.error_classes import UnsupportedEngineFeatureError
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_exprs import (
    SqlGenerateUuidExpression,
    SqlPercentileExpression,
    SqlPercentileFunctionType,
)
from typing_extensions import override

from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.expr_renderer import (
    DefaultSqlExpressionRenderer,
    SqlExpressionRenderer,
    SqlExpressionRenderResult,
)
from metricflow.sql.render.sql_plan_renderer import DefaultSqlPlanRenderer


class SnowflakeSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the Snowflake engine."""

    sql_engine = SqlEngine.SNOWFLAKE

    @property
    @override
    def supported_percentile_function_types(self) -> Collection[SqlPercentileFunctionType]:
        return {
            SqlPercentileFunctionType.CONTINUOUS,
            SqlPercentileFunctionType.DISCRETE,
            SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS,
        }

    @override
    def render_date_part(self, date_part: DatePart) -> str:
        if date_part is DatePart.DOW:
            return "dayofweekiso"

        return super().render_date_part(date_part)

    @override
    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:
        return SqlExpressionRenderResult(
            sql="UUID_STRING()",
            bind_parameter_set=SqlBindParameterSet(),
        )

    @override
    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        """Render a percentile expression for Snowflake."""
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        params = arg_rendered.bind_parameter_set
        percentile = node.percentile_args.percentile

        if node.percentile_args.function_type is SqlPercentileFunctionType.CONTINUOUS:
            function_str = "PERCENTILE_CONT"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.DISCRETE:
            function_str = "PERCENTILE_DISC"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            return SqlExpressionRenderResult(
                sql=f"APPROX_PERCENTILE({arg_rendered.sql}, {percentile})",
                bind_parameter_set=params,
            )
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE:
            raise UnsupportedEngineFeatureError(
                "Approximate discrete percentile aggregate not supported for Snowflake. Set "
                + "use_discrete_percentile and/or use_approximate_percentile to false in all percentile measures."
            )
        else:
            assert_values_exhausted(node.percentile_args.function_type)

        return SqlExpressionRenderResult(
            sql=f"{function_str}({percentile}) WITHIN GROUP (ORDER BY ({arg_rendered.sql}))",
            bind_parameter_set=params,
        )


class SnowflakeSqlPlanRenderer(DefaultSqlPlanRenderer):
    """Plan renderer for the Snowflake engine."""

    EXPR_RENDERER = SnowflakeSqlExpressionRenderer()

    @property
    @override
    def expr_renderer(self) -> SqlExpressionRenderer:
        return self.EXPR_RENDERER
