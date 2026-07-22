from __future__ import annotations

import logging
from typing import Collection

from metricflow_semantics.errors.error_classes import UnsupportedEngineFeatureError
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlDateTruncExpression,
    SqlGenerateUuidExpression,
    SqlPercentileExpression,
    SqlPercentileFunctionType,
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
from metricflow_semantic_interfaces.enum_extension import assert_values_exhausted
from metricflow_semantic_interfaces.type_enums.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


class VerticaSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the Vertica engine."""

    sql_engine = SqlEngine.VERTICA

    @property
    @override
    def double_data_type(self) -> str:
        """Custom double data type for the Vertica engine."""
        return "DOUBLE PRECISION"

    @property
    @override
    def supported_percentile_function_types(self) -> Collection[SqlPercentileFunctionType]:
        """Vertica only supports percentile aggregation through APPROXIMATE_PERCENTILE.

        PERCENTILE_CONT and PERCENTILE_DISC exist in Vertica, but only as analytic (window) functions that
        require an OVER clause, so they can't be rendered as aggregate functions in a GROUP BY query.
        """
        return {SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS}

    @override
    def visit_date_trunc_expr(self, node: SqlDateTruncExpression) -> SqlExpressionRenderResult:
        """Render DATE_TRUNC for Vertica, which uses plural field names for sub-second granularities.

        Reference: https://docs.vertica.com/latest/en/sql-reference/functions/data-type-specific-functions/datetime-functions/date-trunc/
        """
        self._validate_granularity_for_engine(node.time_granularity)

        arg_rendered = self.render_sql_expr(node.arg)

        if node.time_granularity is TimeGranularity.MILLISECOND:
            field = "milliseconds"
        elif node.time_granularity is TimeGranularity.MICROSECOND:
            field = "microseconds"
        else:
            field = node.time_granularity.value

        return SqlExpressionRenderResult(
            sql=f"DATE_TRUNC('{field}', {arg_rendered.sql})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def visit_subtract_time_interval_expr(self, node: SqlSubtractTimeIntervalExpression) -> SqlExpressionRenderResult:
        """Render time delta operations for Vertica, which uses TIMESTAMPADD instead of DATEADD.

        TIMESTAMPADD supports the quarter datepart natively, so no quarter-to-month conversion is needed.
        """
        arg_rendered = node.arg.accept(self)

        return SqlExpressionRenderResult(
            sql=f"TIMESTAMPADD({node.granularity.value}, -{node.count}, {arg_rendered.sql})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def visit_add_time_expr(self, node: SqlAddTimeExpression) -> SqlExpressionRenderResult:
        """Render time delta operations for Vertica, which uses TIMESTAMPADD instead of DATEADD."""
        arg_rendered = node.arg.accept(self)
        count_rendered = node.count_expr.accept(self)
        count_sql = f"({count_rendered.sql})" if node.count_expr.requires_parenthesis else count_rendered.sql

        return SqlExpressionRenderResult(
            sql=f"TIMESTAMPADD({node.granularity.value}, {count_sql}, {arg_rendered.sql})",
            bind_parameter_set=SqlBindParameterSet.merge_iterable(
                (arg_rendered.bind_parameter_set, count_rendered.bind_parameter_set)
            ),
        )

    @override
    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:
        return SqlExpressionRenderResult(
            sql="UUID_GENERATE()",
            bind_parameter_set=SqlBindParameterSet(),
        )

    @override
    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        """Render a percentile expression for Vertica."""
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        params = arg_rendered.bind_parameter_set
        percentile = node.percentile_args.percentile

        if node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            # APPROXIMATE_PERCENTILE is only defined for FLOAT arguments, so cast to cover integer measures.
            return SqlExpressionRenderResult(
                sql=(
                    f"APPROXIMATE_PERCENTILE(CAST({arg_rendered.sql} AS {self.double_data_type}) "
                    f"USING PARAMETERS percentile = {percentile})"
                ),
                bind_parameter_set=params,
            )
        elif (
            node.percentile_args.function_type is SqlPercentileFunctionType.CONTINUOUS
            or node.percentile_args.function_type is SqlPercentileFunctionType.DISCRETE
            or node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE
        ):
            raise UnsupportedEngineFeatureError(
                "Continuous, discrete, and approximate discrete percentile aggregates are not supported for "
                + "Vertica. Set use_approximate_percentile to true and use_discrete_percentile to false in all "
                + "percentile simple-metrics."
            )
        else:
            assert_values_exhausted(node.percentile_args.function_type)


class VerticaSqlPlanRenderer(DefaultSqlPlanRenderer):
    """Plan renderer for the Vertica engine."""

    EXPR_RENDERER = VerticaSqlExpressionRenderer()

    @property
    @override
    def expr_renderer(self) -> SqlExpressionRenderer:
        return self.EXPR_RENDERER
