from __future__ import annotations

import logging
from typing import Collection

from metricflow_semantics.errors.error_classes import UnsupportedEngineFeatureError
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlExtractExpression,
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
from metricflow_semantic_interfaces.type_enums.date_part import DatePart

logger = logging.getLogger(__name__)


class StarRocksSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the StarRocks engine."""

    sql_engine = SqlEngine.STARROCKS

    @property
    @override
    def timestamp_data_type(self) -> str:
        """StarRocks DATETIME covers 1000-01-01 to 9999-12-31, wider than TIMESTAMP (1970-2038)."""
        return "DATETIME"

    @property
    @override
    def supported_percentile_function_types(self) -> Collection[SqlPercentileFunctionType]:
        # StarRocks 4.x does not support the WITHIN GROUP (ORDER BY ...) syntax required
        # for PERCENTILE_CONT / PERCENTILE_DISC; only the approximate variant is available.
        return {
            SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS,
        }

    @override
    def render_date_part(self, date_part: DatePart) -> str:
        """Render DATE PART for an EXTRACT expression.

        Override DOW to use StarRocks' DAYOFWEEK keyword (returns 1=Sunday..7=Saturday).
        The visit_extract_expr override then normalizes this to ISO (1=Monday..7=Sunday).
        """
        if date_part is DatePart.DOW:
            return "DAYOFWEEK"

        return date_part.value

    @override
    def visit_extract_expr(self, node: SqlExtractExpression) -> SqlExpressionRenderResult:
        """Render EXTRACT expressions, normalizing DOW to the ISO standard and DOY to DAYOFYEAR().

        StarRocks does not support EXTRACT(doy FROM col); use DAYOFYEAR(col) instead.
        StarRocks EXTRACT(DAYOFWEEK FROM col) returns 1=Sunday..7=Saturday (MySQL convention).
        MetricFlow requires ISO convention: 1=Monday..7=Sunday. Apply a CASE expression to convert.
        """
        if node.date_part is DatePart.DOY:
            arg_rendered = node.arg.accept(self)
            return SqlExpressionRenderResult(
                sql=f"DAYOFYEAR({arg_rendered.sql})",
                bind_parameter_set=arg_rendered.bind_parameter_set,
            )

        extract_result = super().visit_extract_expr(node)

        if node.date_part is not DatePart.DOW:
            return extract_result

        extract_stmt = extract_result.sql
        case_expr = f"CASE WHEN {extract_stmt} = 1 THEN 7 ELSE {extract_stmt} - 1 END"

        return SqlExpressionRenderResult(
            sql=case_expr,
            bind_parameter_set=extract_result.bind_parameter_set,
        )

    @override
    def visit_subtract_time_interval_expr(self, node: SqlSubtractTimeIntervalExpression) -> SqlExpressionRenderResult:
        """Render time subtraction for StarRocks using MySQL-style DATE_SUB.

        StarRocks supports INTERVAL with QUARTER natively, so no conversion to months is needed.
        """
        arg_rendered = node.arg.accept(self)
        return SqlExpressionRenderResult(
            sql=f"DATE_SUB({arg_rendered.sql}, INTERVAL {node.count} {node.granularity.value.upper()})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def visit_add_time_expr(self, node: SqlAddTimeExpression) -> SqlExpressionRenderResult:
        """Render time addition for StarRocks using MySQL-style DATE_ADD.

        StarRocks supports INTERVAL with QUARTER natively, so no conversion to months is needed.
        """
        arg_rendered = node.arg.accept(self)
        count_rendered = node.count_expr.accept(self)
        count_sql = f"({count_rendered.sql})" if node.count_expr.requires_parenthesis else count_rendered.sql

        return SqlExpressionRenderResult(
            sql=f"DATE_ADD({arg_rendered.sql}, INTERVAL {count_sql} {node.granularity.value.upper()})",
            bind_parameter_set=SqlBindParameterSet.merge_iterable(
                (arg_rendered.bind_parameter_set, count_rendered.bind_parameter_set)
            ),
        )

    @override
    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        """Render a percentile expression for StarRocks."""
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        params = arg_rendered.bind_parameter_set
        percentile = node.percentile_args.percentile

        if node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            return SqlExpressionRenderResult(
                sql=f"PERCENTILE_APPROX({arg_rendered.sql}, {percentile})",
                bind_parameter_set=params,
            )
        elif node.percentile_args.function_type is SqlPercentileFunctionType.CONTINUOUS:
            raise UnsupportedEngineFeatureError(
                "Exact continuous percentile aggregate (WITHIN GROUP syntax) is not supported in StarRocks 4.x. "
                + "Set use_approximate_percentile to true in all percentile simple-metrics."
            )
        elif node.percentile_args.function_type is SqlPercentileFunctionType.DISCRETE:
            raise UnsupportedEngineFeatureError(
                "Exact discrete percentile aggregate (WITHIN GROUP syntax) is not supported in StarRocks 4.x. "
                + "Set use_approximate_percentile to true in all percentile simple-metrics."
            )
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE:
            raise UnsupportedEngineFeatureError(
                "Approximate discrete percentile aggregate not supported for StarRocks. Set "
                + "use_discrete_percentile and/or use_approximate_percentile to false in all percentile simple-metrics."
            )
        else:
            assert_values_exhausted(node.percentile_args.function_type)


class StarRocksSqlPlanRenderer(DefaultSqlPlanRenderer):
    """Plan renderer for the StarRocks engine."""

    EXPR_RENDERER = StarRocksSqlExpressionRenderer()

    @property
    @override
    def expr_renderer(self) -> SqlExpressionRenderer:
        return self.EXPR_RENDERER


StarRocksSqlQueryPlanRenderer = StarRocksSqlPlanRenderer
