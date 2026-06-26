from __future__ import annotations

from datetime import date, datetime

from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_exprs import (
    SqlBetweenExpression,
    SqlStringLiteralExpression,
)
from typing_extensions import override

from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.expr_renderer import SqlExpressionRenderResult
from metricflow.sql.render.trino import TrinoSqlExpressionRenderer, TrinoSqlPlanRenderer


class AthenaSqlExpressionRenderer(TrinoSqlExpressionRenderer):
    """Expression renderer for the Amazon Athena engine."""

    sql_engine = SqlEngine.ATHENA

    @override
    def visit_between_expr(self, node: SqlBetweenExpression) -> SqlExpressionRenderResult:
        """Render a between expression for Athena.

        Only wrap bounds with Athena's TIMESTAMP literal syntax when both bounds are string literals that parse as
        Python fromisoformat-compatible dates or timezone-naive datetimes. This avoids misclassifying arbitrary
        rendered SQL fragments as timestamp literals.
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
    def __is_iso_date_or_timestamp_literal(node: SqlStringLiteralExpression) -> bool:
        """Return True for Python fromisoformat-compatible dates or timezone-naive datetimes."""
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
        start_expr = node.start_expr.as_string_literal_expression
        end_expr = node.end_expr.as_string_literal_expression
        if start_expr is None or end_expr is None:
            return False

        return self.__is_iso_date_or_timestamp_literal(start_expr) and self.__is_iso_date_or_timestamp_literal(end_expr)


class AthenaSqlPlanRenderer(TrinoSqlPlanRenderer):
    """Plan renderer for the Amazon Athena engine."""

    EXPR_RENDERER = AthenaSqlExpressionRenderer()
