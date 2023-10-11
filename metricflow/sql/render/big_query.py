from __future__ import annotations

from fractions import Fraction
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
    SqlCastToTimestampExpression,
    SqlDateTruncExpression,
    SqlGenerateUuidExpression,
    SqlPercentileExpression,
    SqlPercentileFunctionType,
    SqlSubtractTimeIntervalExpression,
)
from metricflow.sql.sql_plan import SqlSelectColumn
from metricflow.time.date_part import DatePart


class BigQuerySqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the BigQuery engine."""

    @property
    @override
    def double_data_type(self) -> str:
        """Custom double data type for BigQuery engine."""
        return "FLOAT64"

    @property
    @override
    def timestamp_data_type(self) -> str:
        """Custom timestamp type override for use in BigQuery.

        We use DATETIME for BigQuery because it is time zone agnostic, which more closely matches the
        runtime behavior of the TIMESTAMP types as used in other engines. This, however, is a choice we
        may need to re-examine in the future.
        """
        return "DATETIME"

    @property
    @override
    def supported_percentile_function_types(self) -> Collection[SqlPercentileFunctionType]:
        return {SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS}

    @override
    def render_group_by_expr(self, group_by_column: SqlSelectColumn) -> SqlExpressionRenderResult:
        """Custom rendering of group by column expressions.

        BigQuery requires group bys to be referenced by alias, rather than duplicating the expression from the SELECT

        e.g.,
          SELECT COALESCE(x, y) AS x_or_y, SUM(1)
          FROM source_table
          GROUP BY x_or_y

        By default we would render GROUP BY COALESCE(x, y) on that last line, and BigQuery will throw an exception
        """
        return SqlExpressionRenderResult(
            sql=group_by_column.column_alias,
            bind_parameters=group_by_column.expr.bind_parameters,
        )

    @override
    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        """Render a percentile expression for BigQuery.

        Use the fraction class to determine numerator and denominator for offset and quantile.
        For example - 0.5 or median would be 1/2 - generate two quantiles and pick the 1-indexed value
        from the result list [min, median, max].
        """
        if node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            arg_rendered = self.render_sql_expr(node.order_by_arg)
            params = arg_rendered.bind_parameters
            percentile = node.percentile_args.percentile

            fraction = Fraction(percentile).limit_denominator()

            return SqlExpressionRenderResult(
                sql=f"APPROX_QUANTILES({arg_rendered.sql}, {fraction.denominator})[OFFSET({fraction.numerator})]",
                bind_parameters=params,
            )
        elif (
            node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE
            or node.percentile_args.function_type is SqlPercentileFunctionType.CONTINUOUS
            or node.percentile_args.function_type is SqlPercentileFunctionType.DISCRETE
        ):
            raise UnsupportedEngineFeatureError(
                "Only approximate continous percentile aggregations are supported for BigQuery. Set "
                + "use_approximate_percentile and disable use_discrete_percentile in all percentile measures."
            )
        else:
            assert_values_exhausted(node.percentile_args.function_type)

    @override
    def visit_cast_to_timestamp_expr(self, node: SqlCastToTimestampExpression) -> SqlExpressionRenderResult:
        """Casts the time value expression to DATETIME.

        BigQuery's TIMESTAMP type requires timezone inputs to convert to and from different formats, whereas its
        DATETIME data type does not. This is different from Databricks, which simply returns and renders inUTC by
        default, or Snowflake which does something user-configurable but defaults to TIMESTAMP_NTZ, or PostgreSQL,
        which adheres to the SQL standard of TIMESTAMP_NTZ.
        """
        arg_rendered = self.render_sql_expr(node.arg)
        return SqlExpressionRenderResult(
            sql=f"CAST({arg_rendered.sql} AS {self.timestamp_data_type})",
            bind_parameters=arg_rendered.bind_parameters,
        )

    @override
    def visit_date_trunc_expr(self, node: SqlDateTruncExpression) -> SqlExpressionRenderResult:
        """Render DATE_TRUNC for BigQuery, which takes the opposite argument order from Snowflake and Redshift."""
        arg_rendered = self.render_sql_expr(node.arg)

        prefix = ""
        if node.time_granularity == TimeGranularity.WEEK:
            prefix = "iso"

        return SqlExpressionRenderResult(
            sql=f"DATE_TRUNC({arg_rendered.sql}, {prefix}{node.time_granularity.value})",
            bind_parameters=arg_rendered.bind_parameters,
        )

    @override
    def render_date_part(self, date_part: DatePart) -> str:
        if date_part == DatePart.DOY:
            return "dayofyear"
        if date_part == DatePart.DOW:
            return "dayofweek"
        if date_part == DatePart.WEEK:
            return "isoweek"

        return super().render_date_part(date_part)

    @override
    def visit_time_delta_expr(self, node: SqlSubtractTimeIntervalExpression) -> SqlExpressionRenderResult:
        """Render time delta for BigQuery, which requires ISO prefixing for the WEEK granularity value."""
        column = node.arg.accept(self)

        return SqlExpressionRenderResult(
            sql=f"DATE_SUB(CAST({column.sql} AS {self.timestamp_data_type}), INTERVAL {node.count} {node.granularity.value})",
            bind_parameters=column.bind_parameters,
        )

    @override
    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:
        return SqlExpressionRenderResult(
            sql="GENERATE_UUID()",
            bind_parameters=SqlBindParameters(),
        )


class BigQuerySqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for the BigQuery engine."""

    EXPR_RENDERER = BigQuerySqlExpressionRenderer()

    @property
    @override
    def expr_renderer(self) -> SqlExpressionRenderer:
        return self.EXPR_RENDERER
