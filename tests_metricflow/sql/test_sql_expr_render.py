from __future__ import annotations

import logging
import textwrap
from typing import List

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.sql.sql_exprs import (
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
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.sql.render.expr_renderer import DefaultSqlExpressionRenderer
from tests_metricflow.snapshot_utils import assert_str_snapshot_equal

logger = logging.getLogger(__name__)


@pytest.fixture
def default_expr_renderer() -> DefaultSqlExpressionRenderer:  # noqa: D103
    return DefaultSqlExpressionRenderer()


def test_str_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D103
    actual = default_expr_renderer.render_sql_expr(SqlStringExpression.create("a + b")).sql
    expected = "a + b"
    assert actual == expected


def test_col_ref_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D103
    actual = default_expr_renderer.render_sql_expr(
        SqlColumnReferenceExpression.create(SqlColumnReference("my_table", "my_col"))
    ).sql
    expected = "my_table.my_col"
    assert actual == expected


def test_comparison_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D103
    actual = default_expr_renderer.render_sql_expr(
        SqlComparisonExpression.create(
            left_expr=SqlColumnReferenceExpression.create(SqlColumnReference("my_table", "my_col")),
            comparison=SqlComparison.EQUALS,
            right_expr=SqlStringExpression.create("a + b"),
        )
    ).sql
    assert actual == "my_table.my_col = (a + b)"


def test_require_parenthesis(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D103
    actual = default_expr_renderer.render_sql_expr(
        SqlComparisonExpression.create(
            left_expr=SqlColumnReferenceExpression.create(SqlColumnReference("a", "booking_value")),
            comparison=SqlComparison.GREATER_THAN,
            right_expr=SqlStringExpression.create("100", requires_parenthesis=False),
        )
    ).sql

    assert actual == "a.booking_value > 100"


def test_function_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D103
    actual = default_expr_renderer.render_sql_expr(
        SqlAggregateFunctionExpression.create(
            sql_function=SqlFunction.SUM,
            sql_function_args=[
                SqlColumnReferenceExpression.create(SqlColumnReference("my_table", "a")),
                SqlColumnReferenceExpression.create(SqlColumnReference("my_table", "b")),
            ],
        )
    ).sql
    assert actual == "SUM(my_table.a, my_table.b)"


def test_distinct_agg_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:
    """Distinct aggregation functions require the insertion of the DISTINCT keyword in the rendered function expr."""
    actual = default_expr_renderer.render_sql_expr(
        SqlAggregateFunctionExpression.create(
            sql_function=SqlFunction.COUNT_DISTINCT,
            sql_function_args=[
                SqlColumnReferenceExpression.create(SqlColumnReference("my_table", "a")),
                SqlColumnReferenceExpression.create(SqlColumnReference("my_table", "b")),
            ],
        )
    ).sql

    assert actual == "COUNT(DISTINCT my_table.a, my_table.b)"


def test_nested_function_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D103
    actual = default_expr_renderer.render_sql_expr(
        SqlAggregateFunctionExpression.create(
            sql_function=SqlFunction.CONCAT,
            sql_function_args=[
                SqlColumnReferenceExpression.create(SqlColumnReference("my_table", "a")),
                SqlAggregateFunctionExpression.create(
                    sql_function=SqlFunction.CONCAT,
                    sql_function_args=[
                        SqlColumnReferenceExpression.create(SqlColumnReference("my_table", "b")),
                        SqlColumnReferenceExpression.create(SqlColumnReference("my_table", "c")),
                    ],
                ),
            ],
        )
    ).sql
    assert actual == "CONCAT(my_table.a, CONCAT(my_table.b, my_table.c))"


def test_null_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D103
    actual = default_expr_renderer.render_sql_expr(SqlNullExpression.create()).sql
    assert actual == "NULL"


def test_and_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D103
    actual = default_expr_renderer.render_sql_expr(
        SqlLogicalExpression.create(
            operator=SqlLogicalOperator.AND,
            args=(
                SqlStringExpression.create("1 < 2", requires_parenthesis=True),
                SqlStringExpression.create("foo", requires_parenthesis=False),
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


def test_long_and_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D103
    actual = default_expr_renderer.render_sql_expr(
        SqlLogicalExpression.create(
            operator=SqlLogicalOperator.AND,
            args=(
                SqlStringExpression.create("some_long_expression1"),
                SqlStringExpression.create("some_long_expression2"),
                SqlStringExpression.create("some_long_expression3"),
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


def test_string_literal_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D103
    actual = default_expr_renderer.render_sql_expr(SqlStringLiteralExpression.create("foo")).sql
    assert actual == "'foo'"


def test_is_null_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D103
    actual = default_expr_renderer.render_sql_expr(
        SqlIsNullExpression.create(SqlStringExpression.create("foo", requires_parenthesis=False))
    ).sql
    assert actual == "foo IS NULL"


def test_date_trunc_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D103
    actual = default_expr_renderer.render_sql_expr(
        SqlDateTruncExpression.create(time_granularity=TimeGranularity.MONTH, arg=SqlStringExpression.create("ds"))
    ).sql
    assert actual == "DATE_TRUNC('month', ds)"


def test_extract_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D103
    actual = default_expr_renderer.render_sql_expr(
        SqlExtractExpression.create(date_part=DatePart.DOY, arg=SqlStringExpression.create("ds"))
    ).sql
    assert actual == "EXTRACT(doy FROM ds)"


def test_ratio_computation_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D103
    actual = default_expr_renderer.render_sql_expr(
        SqlRatioComputationExpression.create(
            numerator=SqlAggregateFunctionExpression.create(
                SqlFunction.SUM,
                sql_function_args=[SqlStringExpression.create(sql_expr="1", requires_parenthesis=False)],
            ),
            denominator=SqlColumnReferenceExpression.create(
                SqlColumnReference(column_name="divide_by_me", table_alias="a")
            ),
        ),
    ).sql
    assert actual == "CAST(SUM(1) AS DOUBLE) / CAST(NULLIF(a.divide_by_me, 0) AS DOUBLE)"


def test_expr_rewrite(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D103
    expr = SqlLogicalExpression.create(
        operator=SqlLogicalOperator.AND,
        args=(
            SqlColumnReferenceExpression.create(SqlColumnReference("a", "col0")),
            SqlColumnReferenceExpression.create(SqlColumnReference("a", "col1")),
        ),
    )

    column_replacements = SqlColumnReplacements(
        {
            SqlColumnReference("a", "col0"): SqlStringExpression.create("foo", requires_parenthesis=False),
            SqlColumnReference("a", "col1"): SqlStringExpression.create("bar", requires_parenthesis=False),
        }
    )
    expr_rewritten = expr.rewrite(column_replacements)
    assert default_expr_renderer.render_sql_expr(expr_rewritten).sql == "foo AND bar"


def test_between_expr(default_expr_renderer: DefaultSqlExpressionRenderer) -> None:  # noqa: D103
    actual = default_expr_renderer.render_sql_expr(
        SqlBetweenExpression.create(
            column_arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "col0")),
            start_expr=SqlCastToTimestampExpression.create(
                arg=SqlStringLiteralExpression.create(
                    literal_value="2020-01-01",
                )
            ),
            end_expr=SqlCastToTimestampExpression.create(
                arg=SqlStringLiteralExpression.create(
                    literal_value="2020-01-10",
                )
            ),
        )
    ).sql
    assert actual == "a.col0 BETWEEN CAST('2020-01-01' AS TIMESTAMP) AND CAST('2020-01-10' AS TIMESTAMP)"


def test_window_function_expr(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    default_expr_renderer: DefaultSqlExpressionRenderer,
) -> None:
    partition_by_args = (
        SqlColumnReferenceExpression.create(SqlColumnReference("b", "col0")),
        SqlColumnReferenceExpression.create(SqlColumnReference("b", "col1")),
    )
    order_by_args = (
        SqlWindowOrderByArgument(
            expr=SqlColumnReferenceExpression.create(SqlColumnReference("a", "col0")),
            descending=True,
            nulls_last=False,
        ),
        SqlWindowOrderByArgument(
            expr=SqlColumnReferenceExpression.create(SqlColumnReference("b", "col0")),
            descending=False,
            nulls_last=True,
        ),
    )

    rendered_sql_lines: List[str] = []

    for num_partition_by_args in range(3):
        rendered_sql_lines.append(f"-- Window function with {num_partition_by_args} PARTITION BY items(s)")
        rendered_sql_lines.append(
            default_expr_renderer.render_sql_expr(
                SqlWindowFunctionExpression.create(
                    sql_function=SqlWindowFunction.FIRST_VALUE,
                    sql_function_args=[SqlColumnReferenceExpression.create(SqlColumnReference("a", "col0"))],
                    partition_by_args=partition_by_args[:num_partition_by_args],
                    order_by_args=(),
                )
            ).sql,
        )
        rendered_sql_lines.append("")

    for num_order_by_args in range(3):
        rendered_sql_lines.append(f"-- Window function with {num_order_by_args} ORDER BY items(s)")
        rendered_sql_lines.append(
            default_expr_renderer.render_sql_expr(
                SqlWindowFunctionExpression.create(
                    sql_function=SqlWindowFunction.FIRST_VALUE,
                    sql_function_args=[SqlColumnReferenceExpression.create(SqlColumnReference("a", "col0"))],
                    partition_by_args=(),
                    order_by_args=order_by_args[:num_order_by_args],
                )
            ).sql,
        )
        rendered_sql_lines.append("")

    rendered_sql_lines.append("-- Window function with PARTITION BY and ORDER BY items")
    rendered_sql_lines.append(
        default_expr_renderer.render_sql_expr(
            SqlWindowFunctionExpression.create(
                sql_function=SqlWindowFunction.FIRST_VALUE,
                sql_function_args=[SqlColumnReferenceExpression.create(SqlColumnReference("a", "col0"))],
                partition_by_args=partition_by_args,
                order_by_args=order_by_args,
            )
        ).sql
    )

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="rendered_sql",
        snapshot_str="\n".join(rendered_sql_lines),
    )
