from __future__ import annotations

import logging
import textwrap

import pytest
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.sql.render.expr_renderer import DefaultSqlExpressionRenderer
from metricflow.sql.sql_exprs import (
    SqlAggregateFunctionExpression,
    SqlBetweenExpression,
    SqlCastToTimestampExpression,
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlColumnReplacements,
    SqlComparison,
    SqlComparisonExpression,
    SqlDateTruncExpression,
    SqlExtractExpression,
    SqlFunction,
    SqlIsNullExpression,
    SqlLogicalExpression,
    SqlLogicalOperator,
    SqlNullExpression,
    SqlRatioComputationExpression,
    SqlStringExpression,
    SqlStringLiteralExpression,
    SqlWindowFunction,
    SqlWindowFunctionExpression,
    SqlWindowOrderByArgument,
)
from metricflow.time.date_part import DatePart

logger = logging.getLogger(__name__)


@pytest.fixture
def default_expr_renderer() -> DefaultSqlExpressionRenderer:  # noqa: D
    return DefaultSqlExpressionRenderer()


def test_str_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D
    actual = default_expr_renderer.render_sql_expr(SqlStringExpression("a + b")).sql
    expected = "a + b"
    assert actual == expected


def test_col_ref_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D
    actual = default_expr_renderer.render_sql_expr(
        SqlColumnReferenceExpression(SqlColumnReference("my_table", "my_col"))
    ).sql
    expected = "my_table.my_col"
    assert actual == expected


def test_comparison_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D
    actual = default_expr_renderer.render_sql_expr(
        SqlComparisonExpression(
            left_expr=SqlColumnReferenceExpression(SqlColumnReference("my_table", "my_col")),
            comparison=SqlComparison.EQUALS,
            right_expr=SqlStringExpression("a + b"),
        )
    ).sql
    assert actual == "my_table.my_col = (a + b)"


def test_require_parenthesis(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D
    actual = default_expr_renderer.render_sql_expr(
        SqlComparisonExpression(
            left_expr=SqlColumnReferenceExpression(SqlColumnReference("a", "booking_value")),
            comparison=SqlComparison.GREATER_THAN,
            right_expr=SqlStringExpression("100", requires_parenthesis=False),
        )
    ).sql

    assert actual == "a.booking_value > 100"


def test_function_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D
    actual = default_expr_renderer.render_sql_expr(
        SqlAggregateFunctionExpression(
            sql_function=SqlFunction.SUM,
            sql_function_args=[
                SqlColumnReferenceExpression(SqlColumnReference("my_table", "a")),
                SqlColumnReferenceExpression(SqlColumnReference("my_table", "b")),
            ],
        )
    ).sql
    assert actual == "SUM(my_table.a, my_table.b)"


def test_distinct_agg_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:
    """Distinct aggregation functions require the insertion of the DISTINCT keyword in the rendered function expr."""
    actual = default_expr_renderer.render_sql_expr(
        SqlAggregateFunctionExpression(
            sql_function=SqlFunction.COUNT_DISTINCT,
            sql_function_args=[
                SqlColumnReferenceExpression(SqlColumnReference("my_table", "a")),
                SqlColumnReferenceExpression(SqlColumnReference("my_table", "b")),
            ],
        )
    ).sql

    assert actual == "COUNT(DISTINCT my_table.a, my_table.b)"


def test_nested_function_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D
    actual = default_expr_renderer.render_sql_expr(
        SqlAggregateFunctionExpression(
            sql_function=SqlFunction.CONCAT,
            sql_function_args=[
                SqlColumnReferenceExpression(SqlColumnReference("my_table", "a")),
                SqlAggregateFunctionExpression(
                    sql_function=SqlFunction.CONCAT,
                    sql_function_args=[
                        SqlColumnReferenceExpression(SqlColumnReference("my_table", "b")),
                        SqlColumnReferenceExpression(SqlColumnReference("my_table", "c")),
                    ],
                ),
            ],
        )
    ).sql
    assert actual == "CONCAT(my_table.a, CONCAT(my_table.b, my_table.c))"


def test_null_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D
    actual = default_expr_renderer.render_sql_expr(SqlNullExpression()).sql
    assert actual == "NULL"


def test_and_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D
    actual = default_expr_renderer.render_sql_expr(
        SqlLogicalExpression(
            operator=SqlLogicalOperator.AND,
            args=(
                SqlStringExpression("1 < 2", requires_parenthesis=True),
                SqlStringExpression("foo", requires_parenthesis=False),
            ),
        )
    ).sql
    assert (
        actual
        == textwrap.dedent(
            """\
            (1 < 2) AND foo
            """
        ).rstrip()
    )


def test_long_and_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D
    actual = default_expr_renderer.render_sql_expr(
        SqlLogicalExpression(
            operator=SqlLogicalOperator.AND,
            args=(
                SqlStringExpression("some_long_expression1"),
                SqlStringExpression("some_long_expression2"),
                SqlStringExpression("some_long_expression3"),
            ),
        )
    ).sql
    assert (
        actual
        == textwrap.dedent(
            """\
            (
              some_long_expression1
            ) AND (
              some_long_expression2
            ) AND (
              some_long_expression3
            )
            """
        ).rstrip()
    )


def test_string_literal_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D
    actual = default_expr_renderer.render_sql_expr(SqlStringLiteralExpression("foo")).sql
    assert actual == "'foo'"


def test_is_null_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D
    actual = default_expr_renderer.render_sql_expr(
        SqlIsNullExpression(SqlStringExpression("foo", requires_parenthesis=False))
    ).sql
    assert actual == "foo IS NULL"


def test_date_trunc_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D
    actual = default_expr_renderer.render_sql_expr(
        SqlDateTruncExpression(time_granularity=TimeGranularity.MONTH, arg=SqlStringExpression("ds"))
    ).sql
    assert actual == "DATE_TRUNC('month', ds)"


def test_extract_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D
    actual = default_expr_renderer.render_sql_expr(
        SqlExtractExpression(date_part=DatePart.DOY, arg=SqlStringExpression("ds"))
    ).sql
    assert actual == "EXTRACT(doy FROM ds)"


def test_ratio_computation_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D
    actual = default_expr_renderer.render_sql_expr(
        SqlRatioComputationExpression(
            numerator=SqlAggregateFunctionExpression(
                SqlFunction.SUM, sql_function_args=[SqlStringExpression(sql_expr="1", requires_parenthesis=False)]
            ),
            denominator=SqlColumnReferenceExpression(SqlColumnReference(column_name="divide_by_me", table_alias="a")),
        ),
    ).sql
    assert actual == "CAST(SUM(1) AS DOUBLE) / CAST(NULLIF(a.divide_by_me, 0) AS DOUBLE)"


def test_expr_rewrite(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D
    expr = SqlLogicalExpression(
        operator=SqlLogicalOperator.AND,
        args=(
            SqlColumnReferenceExpression(SqlColumnReference("a", "col0")),
            SqlColumnReferenceExpression(SqlColumnReference("a", "col1")),
        ),
    )

    column_replacements = SqlColumnReplacements(
        {
            SqlColumnReference("a", "col0"): SqlStringExpression("foo", requires_parenthesis=False),
            SqlColumnReference("a", "col1"): SqlStringExpression("bar", requires_parenthesis=False),
        }
    )
    expr_rewritten = expr.rewrite(column_replacements)
    assert default_expr_renderer.render_sql_expr(expr_rewritten).sql == "foo AND bar"


def test_between_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D
    actual = default_expr_renderer.render_sql_expr(
        SqlBetweenExpression(
            column_arg=SqlColumnReferenceExpression(SqlColumnReference("a", "col0")),
            start_expr=SqlCastToTimestampExpression(
                arg=SqlStringLiteralExpression(
                    literal_value="2020-01-01",
                )
            ),
            end_expr=SqlCastToTimestampExpression(
                arg=SqlStringLiteralExpression(
                    literal_value="2020-01-10",
                )
            ),
        )
    ).sql
    assert actual == "a.col0 BETWEEN CAST('2020-01-01' AS TIMESTAMP) AND CAST('2020-01-10' AS TIMESTAMP)"


def test_window_function_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D
    actual = default_expr_renderer.render_sql_expr(
        SqlWindowFunctionExpression(
            sql_function=SqlWindowFunction.FIRST_VALUE,
            sql_function_args=[SqlColumnReferenceExpression(SqlColumnReference("a", "col0"))],
            partition_by_args=[
                SqlColumnReferenceExpression(SqlColumnReference("b", "col0")),
                SqlColumnReferenceExpression(SqlColumnReference("b", "col1")),
            ],
            order_by_args=[
                SqlWindowOrderByArgument(
                    expr=SqlColumnReferenceExpression(SqlColumnReference("a", "col0")),
                    descending=True,
                    nulls_last=False,
                ),
                SqlWindowOrderByArgument(
                    expr=SqlColumnReferenceExpression(SqlColumnReference("b", "col0")),
                    descending=False,
                    nulls_last=True,
                ),
            ],
        )
    ).sql
    assert (
        actual
        == "first_value(a.col0) OVER (PARTITION BY b.col0, b.col1 ORDER BY a.col0 DESC NULLS FIRST, b.col0 ASC NULLS LAST)"
    )
