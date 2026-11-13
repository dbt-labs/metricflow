"""ClickHouse percentile rendering on Decimal columns.

`quantileExactInclusive` rejects Decimal arguments with ILLEGAL_TYPE_OF_ARGUMENT, and `quantile` returns the
argument's Decimal type, truncating the interpolated result to its scale (e.g. median of 1 and 2 is 1 for
Decimal(18, 0)). Both must produce the true interpolated value on Decimal columns (e.g. currency columns).
"""

from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.sql.sql_exprs import (
    SqlPercentileExpression,
    SqlPercentileExpressionArgument,
    SqlStringExpression,
)
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.snapshot_utils import assert_object_snapshot_equal

logger = logging.getLogger(__name__)


@pytest.mark.clickhouse_only
@pytest.mark.sql_engine_snapshot
def test_continuous_percentile_on_decimal(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
) -> None:
    """Continuous percentiles must return the interpolated value on Decimal and Nullable(Decimal) columns."""
    decimal_source_queries = (
        # Values 0 .. 9.
        "SELECT toDecimal64(number, 0) AS x FROM numbers(10)",
        # Values 1 .. 8 plus two NULLs, which must be ignored.
        "SELECT if(number IN (0, 9), NULL, toDecimal64(number, 0)) AS x FROM numbers(10)",
    )
    results = []
    for approximate in (False, True):
        percentile_expr = SqlPercentileExpression.create(
            order_by_arg=SqlStringExpression.create(sql_expr="x", requires_parenthesis=False),
            percentile_args=SqlPercentileExpressionArgument.create(
                percentile=0.5, discrete=False, approximate=approximate
            ),
        )
        rendered_percentile = sql_client.sql_plan_renderer.expr_renderer.render_sql_expr(percentile_expr).sql
        for decimal_source_query in decimal_source_queries:
            sql = f"SELECT {rendered_percentile} AS percentile FROM ({decimal_source_query})"
            result = sql_client.query(sql).get_cell_value(0, 0)
            # Scale 0 makes truncation visible: an uncast Decimal result would be 4.
            assert result == 4.5, f"Expected 4.5, got {result!r} for: {sql}"
            results.append({"sql": sql, "result": result})

    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj=results,
        sql_client=sql_client,
    )
