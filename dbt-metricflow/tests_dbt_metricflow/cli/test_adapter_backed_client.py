from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator, cast

from dbt.adapters.base import BaseAdapter
from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import AdapterBackedSqlClient, SupportedAdapterTypes

from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.clickhouse import ClickHouseSqlPlanRenderer

logger = logging.getLogger(__name__)


class FakeClickHouseAdapter:
    """Small dbt adapter stand-in that records executed SQL."""

    def __init__(self) -> None:
        self.executed_sql: list[str] = []
        self.executed_fetch_values: list[bool] = []

    def type(self) -> str:
        return "clickhouse"

    @contextmanager
    def connection_named(self, connection_name: str) -> Generator[None, None, None]:
        yield

    def execute(self, sql: str, auto_begin: bool, fetch: bool) -> tuple[str, None]:
        self.executed_sql.append(sql)
        self.executed_fetch_values.append(fetch)
        return ("SUCCESS", None)


def test_clickhouse_adapter_type_maps_to_clickhouse_engine() -> None:
    """Checks that dbt-clickhouse is mapped to the native ClickHouse engine."""
    assert SupportedAdapterTypes.CLICKHOUSE.sql_engine_type is SqlEngine.CLICKHOUSE


def test_clickhouse_adapter_type_maps_to_clickhouse_renderer() -> None:
    """Checks that dbt-clickhouse uses the ClickHouse SQL renderer."""
    assert SupportedAdapterTypes.CLICKHOUSE.sql_plan_renderer.__class__ is ClickHouseSqlPlanRenderer


def test_clickhouse_adapter_dry_run_explains_select() -> None:
    """Checks ClickHouse SELECT dry runs use plan validation."""
    adapter = FakeClickHouseAdapter()
    client = AdapterBackedSqlClient(cast(BaseAdapter, adapter))

    client.dry_run("SELECT 1")

    assert adapter.executed_sql == ["EXPLAIN SELECT 1"]
    assert adapter.executed_fetch_values == [False]


def test_clickhouse_adapter_dry_run_uses_ast_for_non_select() -> None:
    """Checks ClickHouse non-SELECT dry runs avoid executing DDL."""
    adapter = FakeClickHouseAdapter()
    client = AdapterBackedSqlClient(cast(BaseAdapter, adapter))

    client.dry_run("CREATE TABLE foo AS SELECT 1")

    assert adapter.executed_sql == ["EXPLAIN AST CREATE TABLE foo AS SELECT 1"]
    assert adapter.executed_fetch_values == [False]
