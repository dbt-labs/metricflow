from __future__ import annotations

from datetime import date, datetime
from typing import Collection

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlArithmeticExpression,
    SqlArithmeticOperator,
    SqlBetweenExpression,
    SqlGenerateUuidExpression,
    SqlIntegerExpression,
    SqlPercentileExpression,
    SqlPercentileFunctionType,
    SqlStringLiteralExpression,
    SqlSubtractTimeIntervalExpression,
)
from typing_extensions import override

from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.expr_renderer import (
    DefaultSqlExpressionRenderer,
    SqlExpressionRenderer,
    SqlExpressionRenderResult,
)
from metricflow.sql.render.sql_plan_renderer import DefaultSqlPlanRenderer


class AthenaSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the Amazon Athena engine."""

    sql_engine = SqlEngine.ATHENA

    @property
    @override
    def supported_percentile_function_types(self) -> Collection[SqlPercentileFunctionType]:
        return {
            SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS,
        }

    @override
    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:
        return SqlExpressionRenderResult(
            sql="uuid()",
            bind_parameter_set=SqlBindParameterSet(),
        )

    @override
    def visit_subtract_time_interval_expr(self, node: SqlSubtractTimeIntervalExpression) -> SqlExpressionRenderResult:
        """Render time delta for Athena, require granularity in quotes and function name change."""
        arg_rendered = node.arg.accept(self)

        count = node.count
        granularity = node.granularity
        if granularity is TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            count *= 3
        return SqlExpressionRenderResult(
            sql=f"DATE_ADD('{granularity.value}', -{count}, {arg_rendered.sql})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def visit_add_time_expr(self, node: SqlAddTimeExpression) -> SqlExpressionRenderResult:
        """Render time delta for Athena, require granularity in quotes and function name change."""
        granularity = node.granularity
        count_expr = node.count_expr
        if granularity is TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            count_expr = SqlArithmeticExpression.create(
                left_expr=node.count_expr,
                operator=SqlArithmeticOperator.MULTIPLY,
                right_expr=SqlIntegerExpression.create(3),
            )

        arg_rendered = node.arg.accept(self)
        count_rendered = count_expr.accept(self)
        count_sql = f"({count_rendered.sql})" if count_expr.requires_parenthesis else count_rendered.sql

        return SqlExpressionRenderResult(
            sql=f"DATE_ADD('{granularity.value}', {count_sql}, {arg_rendered.sql})",
            bind_parameter_set=SqlBindParameterSet.merge_iterable(
                (arg_rendered.bind_parameter_set, count_rendered.bind_parameter_set)
            ),
        )

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
        """Return True when a string literal is a strict ISO date or datetime literal."""
        literal_value = node.literal_value
        try:
            datetime.fromisoformat(literal_value.replace("Z", "+00:00"))
            return True
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

    @override
    def render_date_part(self, date_part: DatePart) -> str:
        """Render DATE PART for an EXTRACT expression.

        Override DAY_OF_WEEK in Athena to ISO date part to ensure all engines return consistent results.
        """
        if date_part is DatePart.DOW:
            return "DAY_OF_WEEK"

        return date_part.value


class AthenaSqlPlanRenderer(DefaultSqlPlanRenderer):
    """Plan renderer for the Amazon Athena engine."""

    EXPR_RENDERER = AthenaSqlExpressionRenderer()

    @property
    @override
    def expr_renderer(self) -> SqlExpressionRenderer:
        return self.EXPR_RENDERER
