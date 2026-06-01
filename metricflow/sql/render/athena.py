from __future__ import annotations

from datetime import date, datetime

from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_exprs import (
    SqlBetweenExpression,
    SqlPercentileExpression,
    SqlPercentileFunctionType,
    SqlStringLiteralExpression,
)
from typing_extensions import override

from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.expr_renderer import SqlExpressionRenderResult
from metricflow.sql.render.trino import TrinoSqlExpressionRenderer, TrinoSqlPlanRenderer
from metricflow_semantic_interfaces.enum_extension import assert_values_exhausted


class AthenaSqlExpressionRenderer(TrinoSqlExpressionRenderer):
    """Expression renderer for the Amazon Athena engine."""

    sql_engine = SqlEngine.ATHENA

    @override
    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        """Render a percentile expression for Athena."""
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        params = arg_rendered.bind_parameter_set
        percentile = node.percentile_args.percentile

        if node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            return SqlExpressionRenderResult(
                sql=f"approx_percentile({arg_rendered.sql}, {percentile})",
                bind_parameter_set=params,
            )
        elif (
            node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE
            or node.percentile_args.function_type is SqlPercentileFunctionType.DISCRETE
            or node.percentile_args.function_type is SqlPercentileFunctionType.CONTINUOUS
        ):
            raise RuntimeError(
                "Discrete, Continuous and Approximate discrete percentile aggregates are not supported for Athena. Set "
                + "use_approximate_percentile and disable use_discrete_percentile in all percentile simple-metrics."
            )
        else:
            assert_values_exhausted(node.percentile_args.function_type)

    @override
    def visit_between_expr(self, node: SqlBetweenExpression) -> SqlExpressionRenderResult:
        """Render a between expression for Athena.

        Only wrap bounds with Athena's TIMESTAMP literal syntax when both bounds are string literals that strictly
        parse as ISO date or datetime values. This avoids misclassifying arbitrary rendered SQL fragments as
        timestamp literals.
        """
        rendered_column_arg = self.render_sql_expr(node.column_arg)
        rendered_start_expr = self.render_sql_expr(node.start_expr)
        rendered_end_expr = self.render_sql_expr(node.end_expr)

        bind_parameter_set = SqlBindParameterSet()
        bind_parameter_set = bind_parameter_set.merge(rendered_column_arg.bind_parameter_set)
        bind_parameter_set = bind_parameter_set.merge(rendered_start_expr.bind_parameter_set)
        bind_parameter_set = bind_parameter_set.merge(rendered_end_expr.bind_parameter_set)

        if self.__should_wrap_between_bounds_with_timestamp(node):
            sql = (
                f"{rendered_column_arg.sql} BETWEEN timestamp {rendered_start_expr.sql} "
                f"AND timestamp {rendered_end_expr.sql}"
            )
        else:
            sql = f"{rendered_column_arg.sql} BETWEEN {rendered_start_expr.sql} AND {rendered_end_expr.sql}"

        return SqlExpressionRenderResult(
            sql=sql,
            bind_parameter_set=bind_parameter_set,
        )

    @staticmethod
    def __is_iso_timestamp_literal(node: SqlStringLiteralExpression) -> bool:
        """Return True when a string literal is a strict ISO date or timezone-naive datetime literal."""
        literal_value = node.literal_value
        try:
            return datetime.fromisoformat(literal_value).tzinfo is None
        except ValueError:
            try:
                date.fromisoformat(literal_value)
                return True
            except ValueError:
                return False

    def __should_wrap_between_bounds_with_timestamp(self, node: SqlBetweenExpression) -> bool:
        """Check whether both between bounds are safe to render as Athena timestamp literals."""
        if not isinstance(node.start_expr, SqlStringLiteralExpression) or not isinstance(
            node.end_expr, SqlStringLiteralExpression
        ):
            return False

        return self.__is_iso_timestamp_literal(node.start_expr) and self.__is_iso_timestamp_literal(node.end_expr)


class AthenaSqlPlanRenderer(TrinoSqlPlanRenderer):
    """Plan renderer for the Amazon Athena engine."""

    EXPR_RENDERER = AthenaSqlExpressionRenderer()
