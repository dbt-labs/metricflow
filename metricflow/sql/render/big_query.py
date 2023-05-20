from fractions import Fraction

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
    SqlTimeDeltaExpression,
)
from metricflow.sql.sql_plan import SqlSelectColumn
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity


class BigQuerySqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the BigQuery engine."""

    @property
    def double_data_type(self) -> str:
        """Custom double data type for BigQuery engine"""
        return "FLOAT64"

    def render_group_by_expr(self, group_by_column: SqlSelectColumn) -> SqlExpressionRenderResult:
        """Custom rendering of group by column expressions

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
        raise RuntimeError(
            "Only approximate continous percentile aggregations are supported for BigQuery. Set "
            + "use_approximate_percentile and disable use_discrete_percentile in all percentile measures."
        )

    def visit_cast_to_timestamp_expr(self, node: SqlCastToTimestampExpression) -> SqlExpressionRenderResult:
        """Casts the time value expression to DATETIME, as per standard BigQuery preferences."""
        arg_rendered = self.render_sql_expr(node.arg)
        return SqlExpressionRenderResult(
            sql=f"CAST({arg_rendered.sql} AS DATETIME)",
            bind_parameters=arg_rendered.bind_parameters,
        )

    def visit_date_trunc_expr(self, node: SqlDateTruncExpression) -> SqlExpressionRenderResult:
        """Render DATE_TRUNC for BigQuery, which takes the opposite argument order from Snowflake and Redshift"""
        arg_rendered = self.render_sql_expr(node.arg)

        prefix = ""
        if node.time_granularity in (TimeGranularity.WEEK, TimeGranularity.YEAR):
            prefix = "iso"

        return SqlExpressionRenderResult(
            sql=f"DATE_TRUNC({arg_rendered.sql}, {prefix}{node.time_granularity.value})",
            bind_parameters=arg_rendered.bind_parameters,
        )

    def visit_time_delta_expr(self, node: SqlTimeDeltaExpression) -> SqlExpressionRenderResult:  # noqa: D
        column = node.arg.accept(self)
        if node.grain_to_date:
            granularity = node.granularity
            if granularity == TimeGranularity.WEEK or granularity == TimeGranularity.YEAR:
                granularity_value = "ISO" + granularity.value.upper()
            else:
                granularity_value = granularity.value
            return SqlExpressionRenderResult(
                sql=f"DATE_TRUNC({column.sql}, {granularity_value})",
                bind_parameters=column.bind_parameters,
            )

        return SqlExpressionRenderResult(
            sql=f"DATE_SUB(CAST({column.sql} AS DATETIME), INTERVAL {node.count} {node.granularity.value})",
            bind_parameters=column.bind_parameters,
        )

    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:  # noqa: D
        return SqlExpressionRenderResult(
            sql="GENERATE_UUID()",
            bind_parameters=SqlBindParameters(),
        )


class BigQuerySqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for the BigQuery engine."""

    EXPR_RENDERER = BigQuerySqlExpressionRenderer()

    @property
    def expr_renderer(self) -> SqlExpressionRenderer:  # noqa :D
        return self.EXPR_RENDERER
