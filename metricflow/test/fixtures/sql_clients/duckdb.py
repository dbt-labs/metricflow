from __future__ import annotations

import logging
import threading
from typing import Optional

import pandas as pd
import sqlalchemy
from sqlalchemy.pool import StaticPool
from typing_extensions import override

from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.duckdb_renderer import DuckDbSqlQueryPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql_request.sql_request_attributes import SqlJsonTag, SqlRequestTagSet
from metricflow.test.fixtures.sql_clients.common_client import SqlDialect
from metricflow.test.fixtures.sql_clients.sqlalchemy_dialect import SqlAlchemySqlClient

logger = logging.getLogger(__name__)


class DuckDbSqlClient(SqlAlchemySqlClient):
    """Implements DuckDB."""

    @staticmethod
    def from_connection_details(url: str, password: Optional[str] = None) -> SqlAlchemySqlClient:  # noqa: D
        parsed_url = sqlalchemy.engine.url.make_url(url)
        dialect = SqlDialect.DUCKDB.value
        if parsed_url.drivername != dialect:
            raise ValueError(f"Expected dialect '{dialect}' in {url}")

        if password:
            raise ValueError("Password should be empty")

        return DuckDbSqlClient(file_path=parsed_url.database)

    def __init__(self, file_path: Optional[str] = None) -> None:  # noqa: D
        # DuckDB is thread-safe w/ a single connection.
        # https://duckdb.org/docs/archive/0.2.9/api/python.html
        self._concurrency_lock = threading.Lock()
        super().__init__(
            sqlalchemy.create_engine(
                f"duckdb:///{file_path if file_path else ':memory:'}",
                poolclass=StaticPool,
            )
        )

    @property
    @override
    def sql_engine_type(self) -> SqlEngine:
        return SqlEngine.DUCKDB

    @property
    @override
    def sql_query_plan_renderer(self) -> SqlQueryPlanRenderer:
        return DuckDbSqlQueryPlanRenderer()

    def _engine_specific_query_implementation(
        self,
        stmt: str,
        bind_params: SqlBindParameters,
        system_tags: SqlRequestTagSet = SqlRequestTagSet(),
        extra_tags: SqlJsonTag = SqlJsonTag(),
    ) -> pd.DataFrame:
        with self._concurrency_lock:
            return super()._engine_specific_query_implementation(stmt=stmt, bind_params=bind_params)

    def _engine_specific_execute_implementation(
        self,
        stmt: str,
        bind_params: SqlBindParameters,
        system_tags: SqlRequestTagSet = SqlRequestTagSet(),
        extra_tags: SqlJsonTag = SqlJsonTag(),
    ) -> None:
        with self._concurrency_lock:
            return super()._engine_specific_execute_implementation(stmt=stmt, bind_params=bind_params)

    def _engine_specific_dry_run_implementation(self, stmt: str, bind_params: SqlBindParameters) -> None:  # noqa: D
        with self._concurrency_lock:
            return super()._engine_specific_dry_run_implementation(stmt=stmt, bind_params=bind_params)

    def create_table_from_dataframe(  # noqa: D
        self, sql_table: SqlTable, df: pd.DataFrame, chunk_size: Optional[int] = None
    ) -> None:
        with self._concurrency_lock:
            return super().create_table_from_dataframe(
                sql_table=sql_table,
                df=df,
                chunk_size=chunk_size,
            )
