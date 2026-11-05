"""ClickHouse percentile rendering on Decimal columns.

`quantileExactInclusive` rejects Decimal arguments with ILLEGAL_TYPE_OF_ARGUMENT, and `quantile` returns the
argument's Decimal type, truncating the interpolated result to its scale (e.g. median of 1 and 2 is 1 for
Decimal(18, 0)). Both must produce the true interpolated value on Decimal columns (e.g. currency columns).
"""

from __future__ import annotations

import logging

import pytest
from metricflow_semantics.sql.sql_exprs import (
    SqlPercentileExpression,
    SqlPercentileExpressionArgument,
    SqlStringExpression,
)

from metricflow.protocols.sql_client import SqlClient, SqlEngine

logger = logging.getLogger(__name__)


def _skip_if_not_clickhouse(sql_client: SqlClient) -> None:
    if sql_client.sql_engine_type is not SqlEngine.CLICKHOUSE:
        pytest.skip("ClickHouse percentile argument types")


@pytest.mark.parametrize("approximate", [False, True], ids=["exact", "approximate"])
@pytest.mark.parametrize(
    "decimal_expr",
    [
        # Values 0 .. 9.
        "toDecimal64(number, 0)",
        # Values 1 .. 8 plus two NULLs, which must be ignored.
        "if(number IN (0, 9), NULL, toDecimal64(number, 0))",
    ],
    ids=["decimal", "nullable_decimal"],
)
def test_continuous_percentile_on_decimal(sql_client: SqlClient, approximate: bool, decimal_expr: str) -> None:
    """Continuous percentiles must return the interpolated value on Decimal and Nullable(Decimal) columns."""
    _skip_if_not_clickhouse(sql_client)
    percentile_expr = SqlPercentileExpression.create(
        order_by_arg=SqlStringExpression.create(sql_expr="x", requires_parenthesis=False),
        percentile_args=SqlPercentileExpressionArgument.create(percentile=0.5, discrete=False, approximate=approximate),
    )
    rendered_percentile = sql_client.sql_plan_renderer.expr_renderer.render_sql_expr(percentile_expr).sql

    result = sql_client.query(
        f"SELECT {rendered_percentile} AS percentile FROM (SELECT {decimal_expr} AS x FROM numbers(10))"
    )

    assert result.row_count == 1
    assert result.column_count == 1
    # Scale 0 makes truncation visible: an uncast Decimal result would be 4.
    assert result.get_cell_value(0, 0) == 4.5
