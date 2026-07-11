from __future__ import annotations

import pytest
from metricflow_semantics.errors.error_classes import UnsupportedEngineFeatureError
from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlCastToTimestampExpression,
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlDateTruncExpression,
    SqlGenerateUuidExpression,
    SqlIntegerExpression,
    SqlPercentileExpression,
    SqlPercentileExpressionArgument,
    SqlPercentileFunctionType,
    SqlStringLiteralExpression,
    SqlSubtractTimeIntervalExpression,
)

from metricflow.sql.render.vertica import VerticaSqlExpressionRenderer
from metricflow_semantic_interfaces.type_enums.time_granularity import TimeGranularity


@pytest.fixture
def vertica_renderer() -> VerticaSqlExpressionRenderer:
    """Fixture providing the Vertica expression renderer."""
    return VerticaSqlExpressionRenderer()


def test_double_data_type(vertica_renderer: VerticaSqlExpressionRenderer) -> None:
    """Test that Vertica uses DOUBLE PRECISION for double precision floating point."""
    assert vertica_renderer.double_data_type == "DOUBLE PRECISION"


def test_supported_percentile_function_types(vertica_renderer: VerticaSqlExpressionRenderer) -> None:
    """Test that Vertica only supports approximate continuous percentile aggregation."""
    assert SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS in vertica_renderer.supported_percentile_function_types
    assert SqlPercentileFunctionType.CONTINUOUS not in vertica_renderer.supported_percentile_function_types
    assert SqlPercentileFunctionType.DISCRETE not in vertica_renderer.supported_percentile_function_types
    assert SqlPercentileFunctionType.APPROXIMATE_DISCRETE not in vertica_renderer.supported_percentile_function_types


def test_date_trunc_day(vertica_renderer: VerticaSqlExpressionRenderer) -> None:
    """Test DATE_TRUNC for day granularity."""
    expr = SqlDateTruncExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        time_granularity=TimeGranularity.DAY,
    )
    result = vertica_renderer.visit_date_trunc_expr(expr)
    assert result.sql == "DATE_TRUNC('day', a.date_col)"


def test_date_trunc_quarter(vertica_renderer: VerticaSqlExpressionRenderer) -> None:
    """Test DATE_TRUNC for quarter granularity."""
    expr = SqlDateTruncExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        time_granularity=TimeGranularity.QUARTER,
    )
    result = vertica_renderer.visit_date_trunc_expr(expr)
    assert result.sql == "DATE_TRUNC('quarter', a.date_col)"


def test_date_trunc_millisecond(vertica_renderer: VerticaSqlExpressionRenderer) -> None:
    """Test DATE_TRUNC for millisecond granularity, which uses a plural field name in Vertica."""
    expr = SqlDateTruncExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        time_granularity=TimeGranularity.MILLISECOND,
    )
    result = vertica_renderer.visit_date_trunc_expr(expr)
    assert result.sql == "DATE_TRUNC('milliseconds', a.date_col)"


def test_date_trunc_microsecond(vertica_renderer: VerticaSqlExpressionRenderer) -> None:
    """Test DATE_TRUNC for microsecond granularity, which uses a plural field name in Vertica."""
    expr = SqlDateTruncExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        time_granularity=TimeGranularity.MICROSECOND,
    )
    result = vertica_renderer.visit_date_trunc_expr(expr)
    assert result.sql == "DATE_TRUNC('microseconds', a.date_col)"


def test_date_trunc_nanosecond_raises(vertica_renderer: VerticaSqlExpressionRenderer) -> None:
    """Test that DATE_TRUNC for nanosecond granularity raises since Vertica has microsecond precision."""
    expr = SqlDateTruncExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        time_granularity=TimeGranularity.NANOSECOND,
    )
    with pytest.raises(UnsupportedEngineFeatureError):
        vertica_renderer.visit_date_trunc_expr(expr)


def test_add_time_months(vertica_renderer: VerticaSqlExpressionRenderer) -> None:
    """Test adding months."""
    expr = SqlAddTimeExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        count_expr=SqlIntegerExpression.create(3),
        granularity=TimeGranularity.MONTH,
    )
    result = vertica_renderer.visit_add_time_expr(expr)
    assert result.sql == "TIMESTAMPADD(month, 3, a.date_col)"


def test_add_time_quarters(vertica_renderer: VerticaSqlExpressionRenderer) -> None:
    """Test adding quarters, which TIMESTAMPADD supports natively without conversion to months."""
    expr = SqlAddTimeExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        count_expr=SqlIntegerExpression.create(2),
        granularity=TimeGranularity.QUARTER,
    )
    result = vertica_renderer.visit_add_time_expr(expr)
    assert result.sql == "TIMESTAMPADD(quarter, 2, a.date_col)"


def test_subtract_time_days(vertica_renderer: VerticaSqlExpressionRenderer) -> None:
    """Test subtracting days."""
    expr = SqlSubtractTimeIntervalExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        count=7,
        granularity=TimeGranularity.DAY,
    )
    result = vertica_renderer.visit_subtract_time_interval_expr(expr)
    assert result.sql == "TIMESTAMPADD(day, -7, a.date_col)"


def test_subtract_time_quarters(vertica_renderer: VerticaSqlExpressionRenderer) -> None:
    """Test subtracting quarters, which TIMESTAMPADD supports natively without conversion to months."""
    expr = SqlSubtractTimeIntervalExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "date_col")),
        count=1,
        granularity=TimeGranularity.QUARTER,
    )
    result = vertica_renderer.visit_subtract_time_interval_expr(expr)
    assert result.sql == "TIMESTAMPADD(quarter, -1, a.date_col)"


def test_percentile_approximate_continuous(vertica_renderer: VerticaSqlExpressionRenderer) -> None:
    """Test approximate continuous percentile using APPROXIMATE_PERCENTILE."""
    expr = SqlPercentileExpression.create(
        order_by_arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "value_col")),
        percentile_args=SqlPercentileExpressionArgument(
            percentile=0.5, function_type=SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS
        ),
    )
    result = vertica_renderer.visit_percentile_expr(expr)
    assert (
        result.sql == "APPROXIMATE_PERCENTILE(CAST(a.value_col AS DOUBLE PRECISION) USING PARAMETERS percentile = 0.5)"
    )


@pytest.mark.parametrize(
    "function_type",
    [
        SqlPercentileFunctionType.CONTINUOUS,
        SqlPercentileFunctionType.DISCRETE,
        SqlPercentileFunctionType.APPROXIMATE_DISCRETE,
    ],
)
def test_unsupported_percentile_types_raise(
    vertica_renderer: VerticaSqlExpressionRenderer, function_type: SqlPercentileFunctionType
) -> None:
    """Test that percentile function types without an aggregate implementation in Vertica raise."""
    expr = SqlPercentileExpression.create(
        order_by_arg=SqlColumnReferenceExpression.create(SqlColumnReference("a", "value_col")),
        percentile_args=SqlPercentileExpressionArgument(percentile=0.5, function_type=function_type),
    )
    with pytest.raises(UnsupportedEngineFeatureError):
        vertica_renderer.visit_percentile_expr(expr)


def test_generate_uuid(vertica_renderer: VerticaSqlExpressionRenderer) -> None:
    """Test UUID generation."""
    expr = SqlGenerateUuidExpression.create()
    result = vertica_renderer.visit_generate_uuid_expr(expr)
    assert result.sql == "UUID_GENERATE()"


def test_cast_to_timestamp(vertica_renderer: VerticaSqlExpressionRenderer) -> None:
    """Test casting to timestamp uses the default TIMESTAMP type."""
    expr = SqlCastToTimestampExpression.create(
        arg=SqlStringLiteralExpression.create("2020-01-01"),
    )
    result = vertica_renderer.visit_cast_to_timestamp_expr(expr)
    assert result.sql == "CAST('2020-01-01' AS TIMESTAMP)"
