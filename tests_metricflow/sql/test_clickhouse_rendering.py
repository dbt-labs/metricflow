from __future__ import annotations

import logging

import pytest
from metricflow_semantics.errors.error_classes import UnsupportedEngineFeatureError
from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlCastToTimestampExpression,
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlDateTruncExpression,
    SqlExtractExpression,
    SqlGenerateUuidExpression,
    SqlIntegerExpression,
    SqlPercentileExpression,
    SqlPercentileExpressionArgument,
    SqlPercentileFunctionType,
    SqlStringLiteralExpression,
    SqlSubtractTimeIntervalExpression,
)

from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.clickhouse import ClickHouseSqlExpressionRenderer, clickhouse_dry_run_statement
from metricflow_semantic_interfaces.type_enums.date_part import DatePart
from metricflow_semantic_interfaces.type_enums.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


def test_clickhouse_engine_unsupported_granularities() -> None:
    """Checks ClickHouse timestamp precision capability metadata."""
    assert SqlEngine.CLICKHOUSE.unsupported_granularities == {TimeGranularity.NANOSECOND}


@pytest.mark.parametrize(
    ("stmt", "expected"),
    (
        ("SELECT 1", "EXPLAIN SELECT 1"),
        ("WITH cte AS (SELECT 1) SELECT * FROM cte", "EXPLAIN WITH cte AS (SELECT 1) SELECT * FROM cte"),
        ("CREATE TABLE foo AS SELECT 1", "EXPLAIN AST CREATE TABLE foo AS SELECT 1"),
    ),
)
def test_clickhouse_dry_run_statement(stmt: str, expected: str) -> None:
    """Checks ClickHouse dry-run statements validate read queries without executing DDL."""
    assert clickhouse_dry_run_statement(stmt) == expected


def test_clickhouse_renders_timestamp_cast() -> None:
    """Checks the timestamp type used for ClickHouse casts."""
    renderer = ClickHouseSqlExpressionRenderer()

    rendered = renderer.render_sql_expr(
        SqlCastToTimestampExpression.create(arg=SqlStringLiteralExpression.create("2020-01-01"))
    )

    assert rendered.sql == "CAST('2020-01-01' AS DateTime64(6))"


def test_clickhouse_renders_uuid() -> None:
    """Checks ClickHouse UUID generation."""
    renderer = ClickHouseSqlExpressionRenderer()

    rendered = renderer.render_sql_expr(SqlGenerateUuidExpression.create())

    assert rendered.sql == "generateUUIDv4()"


def test_clickhouse_renders_date_trunc() -> None:
    """Checks ClickHouse date truncation syntax."""
    renderer = ClickHouseSqlExpressionRenderer()

    rendered = renderer.render_sql_expr(
        SqlDateTruncExpression.create(
            time_granularity=TimeGranularity.DAY,
            arg=SqlColumnReferenceExpression.create(SqlColumnReference(table_alias="a", column_name="ts")),
        )
    )

    assert rendered.sql == "dateTrunc('day', a.ts)"


def test_clickhouse_renders_day_of_week_as_iso() -> None:
    """Checks ClickHouse ISO day-of-week rendering."""
    renderer = ClickHouseSqlExpressionRenderer()

    rendered = renderer.render_sql_expr(
        SqlExtractExpression.create(
            date_part=DatePart.DOW,
            arg=SqlColumnReferenceExpression.create(SqlColumnReference(table_alias="a", column_name="ts")),
        )
    )

    assert rendered.sql == "toDayOfWeek(a.ts)"


def test_clickhouse_renders_day_of_year() -> None:
    """Checks ClickHouse day-of-year rendering."""
    renderer = ClickHouseSqlExpressionRenderer()

    rendered = renderer.render_sql_expr(
        SqlExtractExpression.create(
            date_part=DatePart.DOY,
            arg=SqlColumnReferenceExpression.create(SqlColumnReference(table_alias="a", column_name="ts")),
        )
    )

    assert rendered.sql == "toDayOfYear(a.ts)"


def test_clickhouse_renders_subtract_quarter_as_months() -> None:
    """Checks ClickHouse subtraction for quarter granularity."""
    renderer = ClickHouseSqlExpressionRenderer()

    rendered = renderer.render_sql_expr(
        SqlSubtractTimeIntervalExpression.create(
            arg=SqlColumnReferenceExpression.create(SqlColumnReference(table_alias="a", column_name="ts")),
            count=1,
            granularity=TimeGranularity.QUARTER,
        )
    )

    assert rendered.sql == "dateAdd('month', -3, a.ts)"


def test_clickhouse_renders_add_quarter_as_months() -> None:
    """Checks ClickHouse addition for quarter granularity."""
    renderer = ClickHouseSqlExpressionRenderer()

    rendered = renderer.render_sql_expr(
        SqlAddTimeExpression.create(
            arg=SqlColumnReferenceExpression.create(SqlColumnReference(table_alias="a", column_name="ts")),
            count_expr=SqlIntegerExpression.create(2),
            granularity=TimeGranularity.QUARTER,
        )
    )

    assert rendered.sql == "dateAdd('month', (2 * 3), a.ts)"


def test_clickhouse_renders_approximate_continuous_percentile() -> None:
    """Checks ClickHouse percentile rendering."""
    renderer = ClickHouseSqlExpressionRenderer()

    rendered = renderer.render_sql_expr(
        SqlPercentileExpression.create(
            order_by_arg=SqlColumnReferenceExpression.create(SqlColumnReference(table_alias="a", column_name="value")),
            percentile_args=SqlPercentileExpressionArgument(
                percentile=0.5,
                function_type=SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS,
            ),
        )
    )

    assert rendered.sql == "quantile(0.5)(a.value)"


def test_clickhouse_rejects_exact_percentile() -> None:
    """Checks unsupported ClickHouse percentile variants fail explicitly."""
    renderer = ClickHouseSqlExpressionRenderer()

    with pytest.raises(UnsupportedEngineFeatureError):
        renderer.render_sql_expr(
            SqlPercentileExpression.create(
                order_by_arg=SqlColumnReferenceExpression.create(
                    SqlColumnReference(table_alias="a", column_name="value")
                ),
                percentile_args=SqlPercentileExpressionArgument(
                    percentile=0.5,
                    function_type=SqlPercentileFunctionType.CONTINUOUS,
                ),
            )
        )
