from __future__ import annotations

import pytest
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

from metricflow.sql.render.clickhouse import (
    ClickHouseSqlExpressionRenderer,
    ClickHouseSqlPlanRenderer,
    clickhouse_explain_statement,
    ensure_join_use_nulls_setting,
    sql_has_join_use_nulls_setting,
)
from metricflow.sql.sql_plan import SqlPlan
from metricflow.sql.sql_select_text_node import SqlSelectTextNode
from metricflow_semantic_interfaces.type_enums.date_part import DatePart
from metricflow_semantic_interfaces.type_enums.time_granularity import TimeGranularity


@pytest.fixture
def clickhouse_renderer() -> ClickHouseSqlExpressionRenderer:
    """Fixture providing ClickHouse expression renderer."""
    return ClickHouseSqlExpressionRenderer()


def test_double_data_type(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """CAST targets must be Nullable so CAST of SQL NULL after outer joins succeeds."""
    assert clickhouse_renderer.double_data_type == "Nullable(Float64)"


def test_timestamp_data_type(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """CAST targets must be Nullable so CAST of SQL NULL after outer joins succeeds."""
    assert clickhouse_renderer.timestamp_data_type == "Nullable(DateTime64(3))"


def test_supported_percentile_function_types(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test that ClickHouse supports all percentile function types."""
    assert SqlPercentileFunctionType.CONTINUOUS in clickhouse_renderer.supported_percentile_function_types
    assert SqlPercentileFunctionType.DISCRETE in clickhouse_renderer.supported_percentile_function_types
    assert SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS in clickhouse_renderer.supported_percentile_function_types
    assert SqlPercentileFunctionType.APPROXIMATE_DISCRETE in clickhouse_renderer.supported_percentile_function_types


def test_date_trunc_day(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test DATE_TRUNC for day granularity."""
    expr = SqlDateTruncExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        time_granularity=TimeGranularity.DAY,
    )
    result = clickhouse_renderer.visit_date_trunc_expr(expr)
    assert result.sql == "toStartOfDay(a.date_col)"


def test_date_trunc_week(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test DATE_TRUNC for week granularity (should use mode 1 for ISO week)."""
    expr = SqlDateTruncExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        time_granularity=TimeGranularity.WEEK,
    )
    result = clickhouse_renderer.visit_date_trunc_expr(expr)
    assert result.sql == "toStartOfWeek(a.date_col, 1)"


def test_date_trunc_month(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test DATE_TRUNC for month granularity."""
    expr = SqlDateTruncExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        time_granularity=TimeGranularity.MONTH,
    )
    result = clickhouse_renderer.visit_date_trunc_expr(expr)
    assert result.sql == "toStartOfMonth(a.date_col)"


def test_date_trunc_quarter(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test DATE_TRUNC for quarter granularity."""
    expr = SqlDateTruncExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        time_granularity=TimeGranularity.QUARTER,
    )
    result = clickhouse_renderer.visit_date_trunc_expr(expr)
    assert result.sql == "toStartOfQuarter(a.date_col)"


def test_date_trunc_year(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test DATE_TRUNC for year granularity."""
    expr = SqlDateTruncExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        time_granularity=TimeGranularity.YEAR,
    )
    result = clickhouse_renderer.visit_date_trunc_expr(expr)
    assert result.sql == "toStartOfYear(a.date_col)"


def test_extract_year(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test EXTRACT for year."""
    expr = SqlExtractExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        date_part=DatePart.YEAR,
    )
    result = clickhouse_renderer.visit_extract_expr(expr)
    assert result.sql == "toYear(a.date_col)"


def test_extract_month(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test EXTRACT for month."""
    expr = SqlExtractExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        date_part=DatePart.MONTH,
    )
    result = clickhouse_renderer.visit_extract_expr(expr)
    assert result.sql == "toMonth(a.date_col)"


def test_extract_day_of_week(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test EXTRACT for day of week."""
    expr = SqlExtractExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        date_part=DatePart.DOW,
    )
    result = clickhouse_renderer.visit_extract_expr(expr)
    assert result.sql == "toDayOfWeek(a.date_col, 0)"


def test_add_time_days(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test adding days."""
    expr = SqlAddTimeExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        count_expr=SqlIntegerExpression.create(7),
        granularity=TimeGranularity.DAY,
    )
    result = clickhouse_renderer.visit_add_time_expr(expr)
    assert result.sql == "addDays(a.date_col, 7)"


def test_add_time_weeks(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test adding weeks (should multiply by 7 and use addDays)."""
    expr = SqlAddTimeExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        count_expr=SqlIntegerExpression.create(2),
        granularity=TimeGranularity.WEEK,
    )
    result = clickhouse_renderer.visit_add_time_expr(expr)
    assert result.sql == "addDays(a.date_col, (2) * 7)"


def test_add_time_months(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test adding months."""
    expr = SqlAddTimeExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        count_expr=SqlIntegerExpression.create(3),
        granularity=TimeGranularity.MONTH,
    )
    result = clickhouse_renderer.visit_add_time_expr(expr)
    assert result.sql == "addMonths(a.date_col, 3)"


def test_add_time_quarters(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test adding quarters (should multiply by 3 and use addMonths)."""
    expr = SqlAddTimeExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        count_expr=SqlIntegerExpression.create(2),
        granularity=TimeGranularity.QUARTER,
    )
    result = clickhouse_renderer.visit_add_time_expr(expr)
    assert result.sql == "addMonths(a.date_col, (2) * 3)"


def test_subtract_time_days(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test subtracting days (should use negative addDays)."""
    expr = SqlSubtractTimeIntervalExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        count=7,
        granularity=TimeGranularity.DAY,
    )
    result = clickhouse_renderer.visit_subtract_time_interval_expr(expr)
    assert result.sql == "addDays(a.date_col, -7)"


def test_subtract_time_weeks(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test subtracting weeks (should multiply by 7 and use negative addDays)."""
    expr = SqlSubtractTimeIntervalExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        count=2,
        granularity=TimeGranularity.WEEK,
    )
    result = clickhouse_renderer.visit_subtract_time_interval_expr(expr)
    assert result.sql == "addDays(a.date_col, -14)"


def test_percentile_approximate_continuous(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test approximate continuous percentile (should use quantile)."""
    expr = SqlPercentileExpression.create(
        order_by_arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "value_col")),
        percentile_args=SqlPercentileExpressionArgument(
            percentile=0.5, function_type=SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS
        ),
    )
    result = clickhouse_renderer.visit_percentile_expr(expr)
    assert result.sql == "quantile(0.5)(a.value_col)"


def test_percentile_continuous(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test exact continuous percentile (should use quantileExact)."""
    expr = SqlPercentileExpression.create(
        order_by_arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "value_col")),
        percentile_args=SqlPercentileExpressionArgument(
            percentile=0.5, function_type=SqlPercentileFunctionType.CONTINUOUS
        ),
    )
    result = clickhouse_renderer.visit_percentile_expr(expr)
    assert result.sql == "quantileExact(0.5)(a.value_col)"


def test_percentile_discrete(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test discrete percentile (should use quantileExactLow)."""
    expr = SqlPercentileExpression.create(
        order_by_arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "value_col")),
        percentile_args=SqlPercentileExpressionArgument(
            percentile=0.5, function_type=SqlPercentileFunctionType.DISCRETE
        ),
    )
    result = clickhouse_renderer.visit_percentile_expr(expr)
    assert result.sql == "quantileExactLow(0.5)(a.value_col)"


def test_percentile_approximate_discrete(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test approximate discrete percentile (should use quantileTDigest)."""
    expr = SqlPercentileExpression.create(
        order_by_arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "value_col")),
        percentile_args=SqlPercentileExpressionArgument(
            percentile=0.5, function_type=SqlPercentileFunctionType.APPROXIMATE_DISCRETE
        ),
    )
    result = clickhouse_renderer.visit_percentile_expr(expr)
    assert result.sql == "quantileTDigest(0.5)(a.value_col)"


def test_generate_uuid(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test UUID generation."""
    expr = SqlGenerateUuidExpression.create()
    result = clickhouse_renderer.visit_generate_uuid_expr(expr)
    assert result.sql == "generateUUIDv4()"


def test_cast_to_timestamp(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test casting to timestamp."""
    expr = SqlCastToTimestampExpression.create(
        arg=SqlStringLiteralExpression.create("2020-01-01"),
    )
    result = clickhouse_renderer.visit_cast_to_timestamp_expr(expr)
    assert result.sql == "CAST('2020-01-01' AS Nullable(DateTime64(3)))"


def test_render_date_part_year(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test rendering date part for year."""
    assert clickhouse_renderer.render_date_part(DatePart.YEAR) == "toYear"


def test_render_date_part_month(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test rendering date part for month."""
    assert clickhouse_renderer.render_date_part(DatePart.MONTH) == "toMonth"


def test_render_date_part_day_of_week(clickhouse_renderer: ClickHouseSqlExpressionRenderer) -> None:
    """Test rendering date part for day of week."""
    assert clickhouse_renderer.render_date_part(DatePart.DOW) == "toDayOfWeek"


def test_plan_renderer_emits_join_use_nulls_setting() -> None:
    """Compiled ClickHouse SQL must set join_use_nulls so unmatched outer-join cells are SQL NULL."""
    plan = SqlPlan(render_node=SqlSelectTextNode.create(select_query="SELECT 1 AS x"))
    result = ClickHouseSqlPlanRenderer().render_sql_plan(plan)
    assert result.sql.rstrip().endswith("SETTINGS join_use_nulls = 1")
    assert result.sql.count("SETTINGS join_use_nulls = 1") == 1


def test_ensure_join_use_nulls_setting_ignores_identifier_in_select_list() -> None:
    """A SELECT-list string must not suppress the trailing SETTINGS clause."""
    sql = "SELECT 'join_use_nulls' AS x"
    assert not sql_has_join_use_nulls_setting(sql)
    ensured = ensure_join_use_nulls_setting(sql)
    assert ensured.endswith("SETTINGS join_use_nulls = 1")
    assert ensure_join_use_nulls_setting(ensured) == ensured


def test_ensure_join_use_nulls_setting_merges_into_existing_settings() -> None:
    """ClickHouse allows one SETTINGS list; merge rather than appending a second clause."""
    sql = "SELECT 1 AS x\nSETTINGS max_threads = 2"
    assert ensure_join_use_nulls_setting(sql) == "SELECT 1 AS x\nSETTINGS max_threads = 2, join_use_nulls = 1"


def test_ensure_join_use_nulls_setting_keeps_existing_assignment() -> None:
    """Do not duplicate join_use_nulls when a SETTINGS clause already sets it."""
    sql = "SELECT 1 AS x\nSETTINGS join_use_nulls = 1"
    assert ensure_join_use_nulls_setting(sql) == sql


def test_clickhouse_explain_statement_select_uses_query_tree() -> None:
    """SELECT/WITH dry-run uses EXPLAIN QUERY TREE and strips a trailing semicolon."""
    assert clickhouse_explain_statement("  SELECT 1 AS x;\n") == "EXPLAIN QUERY TREE SELECT 1 AS x"
    assert clickhouse_explain_statement("WITH x AS (SELECT 1) SELECT * FROM x").startswith("EXPLAIN QUERY TREE WITH")


def test_clickhouse_explain_statement_ddl_uses_syntax() -> None:
    """DDL such as CREATE TABLE AS is not a query tree."""
    assert clickhouse_explain_statement("CREATE TABLE t AS SELECT 1") == ("EXPLAIN SYNTAX CREATE TABLE t AS SELECT 1")
