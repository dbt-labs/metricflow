import logging
import threading
from typing import ClassVar, Optional

import pandas as pd
import sqlalchemy
from sqlalchemy.pool import StaticPool

from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SupportedSqlEngine, SqlEngineAttributes
from metricflow.sql.render.duckdb_renderer import DuckDbSqlQueryPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql_clients.common_client import SqlDialect
from metricflow.sql_clients.sqlalchemy_dialect import SqlAlchemySqlClient

logger = logging.getLogger(__name__)


class DuckDbEngineAttributes(SqlEngineAttributes):
    """Engine-specific attributes for the DuckDb query engine"""

    sql_engine_type: ClassVar[SupportedSqlEngine] = SupportedSqlEngine.DUCKDB

    # SQL Engine capabilities
    date_trunc_supported: ClassVar[bool] = True
    full_outer_joins_supported: ClassVar[bool] = True
    indexes_supported: ClassVar[bool] = True
    multi_threading_supported: ClassVar[bool] = True
    timestamp_type_supported: ClassVar[bool] = True
    timestamp_to_string_comparison_supported: ClassVar[bool] = True
    # Cancelling should be possible, but not yet implemented.
    cancel_submitted_queries_supported: ClassVar[bool] = False

    # SQL Dialect replacement strings
    double_data_type_name: ClassVar[str] = "DOUBLE"
    timestamp_type_name: ClassVar[Optional[str]] = "TIMESTAMP"

    # MetricFlow attributes
    sql_query_plan_renderer: ClassVar[SqlQueryPlanRenderer] = DuckDbSqlQueryPlanRenderer()


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
        # DuckDB is not designed with concurrency, but in can work in multi-threaded settings with
        # check_same_thread=False, StaticPool, and serializing of queries via a lock.
        self._concurrency_lock = threading.RLock()
        super().__init__(
            sqlalchemy.create_engine(
                f"duckdb:///{file_path if file_path else ':memory:'}",
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
            )
        )

    @property
    def sql_engine_attributes(self) -> SqlEngineAttributes:
        """Collection of attributes and features specific to the Snowflake SQL engine"""
        return DuckDbEngineAttributes()

    def cancel_submitted_queries(self) -> None:  # noqa: D
        raise NotImplementedError

    def _engine_specific_query_implementation(  # noqa: D
        self, stmt: str, bind_params: SqlBindParameters
    ) -> pd.DataFrame:
        with self._concurrency_lock:
            return super()._engine_specific_query_implementation(stmt=stmt, bind_params=bind_params)

    def _engine_specific_execute_implementation(self, stmt: str, bind_params: SqlBindParameters) -> None:  # noqa: D
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
