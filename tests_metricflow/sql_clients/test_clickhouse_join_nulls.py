"""ClickHouse outer-join NULL contract.

ClickHouse fills unmatched LEFT/FULL OUTER JOIN cells with type defaults (0, '')
unless ``join_use_nulls = 1``. MetricFlow fill-nulls / ratio metrics assume SQL NULL.

The production contract is query-level ``SETTINGS`` on compiled SQL — not session
``SET`` and not a connect-time URL parameter. These tests pin that layering.
"""

from __future__ import annotations

import logging

import pytest

from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.render.clickhouse import ClickHouseSqlPlanRenderer
from metricflow.sql.sql_plan import SqlPlan
from metricflow.sql.sql_select_text_node import SqlSelectTextNode

logger = logging.getLogger(__name__)

_UNMATCHED_LEFT_JOIN = """
SELECT t2.x AS x
FROM (SELECT 1 AS id) AS t1
LEFT JOIN (SELECT 2 AS id, 5 AS x) AS t2 ON t1.id = t2.id
"""


def _extract_data_table_value(data_table: MetricFlowDataTable) -> object:
    assert data_table.row_count == 1
    assert data_table.column_count == 1
    return data_table.get_cell_value(0, 0)


@pytest.mark.clickhouse_only
def test_unmatched_left_join_is_zero_without_query_settings(sql_client: SqlClient) -> None:
    """Default ClickHouse fills unmatched numeric cells with 0, not SQL NULL."""
    result = sql_client.query(_UNMATCHED_LEFT_JOIN)
    assert _extract_data_table_value(result) == 0


@pytest.mark.clickhouse_only
def test_unmatched_left_join_is_sql_null_with_query_settings(sql_client: SqlClient) -> None:
    """Query-level SETTINGS is sufficient; no session SET is required."""
    result = sql_client.query(f"{_UNMATCHED_LEFT_JOIN}\nSETTINGS join_use_nulls = 1")
    assert _extract_data_table_value(result) is None


@pytest.mark.clickhouse_only
def test_rendered_plan_settings_are_sufficient_for_sql_null(sql_client: SqlClient) -> None:
    """The plan renderer is the production contract: execute its SQL as-is."""
    plan = SqlPlan(render_node=SqlSelectTextNode.create(select_query=_UNMATCHED_LEFT_JOIN.strip()))
    sql = ClickHouseSqlPlanRenderer().render_sql_plan(plan).sql
    result = sql_client.query(sql)
    assert _extract_data_table_value(result) is None


@pytest.mark.clickhouse_only
def test_dry_run_accepts_rendered_sql_with_settings(sql_client: SqlClient) -> None:
    """EXPLAIN QUERY TREE must accept compiled SQL that already ends in SETTINGS."""
    plan = SqlPlan(render_node=SqlSelectTextNode.create(select_query=_UNMATCHED_LEFT_JOIN.strip()))
    sql_client.dry_run(ClickHouseSqlPlanRenderer().render_sql_plan(plan).sql)


@pytest.mark.clickhouse_only
def test_exact_continuous_percentile_interpolates(sql_client: SqlClient) -> None:
    """The exact continuous function must interpolate an even-sized input."""
    result = sql_client.query("SELECT quantileExactInclusive(0.5)(number) AS percentile FROM numbers(2)")
    assert _extract_data_table_value(result) == 0.5
