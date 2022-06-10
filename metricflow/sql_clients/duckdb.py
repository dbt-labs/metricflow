import logging
from typing import ClassVar, Optional

import sqlalchemy

from metricflow.protocols.sql_client import SupportedSqlEngine, SqlEngineAttributes
from metricflow.sql.render.duckdb_renderer import DuckDbSqlQueryPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql_clients.common_client import SqlDialect
from metricflow.sql_clients.sqlalchemy_dialect import SqlAlchemySqlClient

logger = logging.getLogger(__name__)


class DuckDbEngineAttributes(SqlEngineAttributes):
    """Engine-specific attributes for the DckDb query engine"""

    sql_engine_type: ClassVar[SupportedSqlEngine] = SupportedSqlEngine.DUCKDB

    # SQL Engine capabilities
    date_trunc_supported: ClassVar[bool] = True
    full_outer_joins_supported: ClassVar[bool] = True
    indexes_supported: ClassVar[bool] = True
    multi_threading_supported: ClassVar[bool] = False
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
    """Implements Redshift."""

    @staticmethod
    def from_connection_details(url: str, password: Optional[str] = None) -> SqlAlchemySqlClient:  # noqa: D
        expected_url = f"{SqlDialect.DUCKDB.value}://"
        if url != expected_url:
            raise ValueError(f"URL was '{url}' but should be '{expected_url}'")
        if password:
            raise ValueError("Password should be empty")

        return DuckDbSqlClient()

    def __init__(self) -> None:  # noqa: D
        super().__init__(sqlalchemy.create_engine("duckdb:///:memory:"))

    @property
    def sql_engine_attributes(self) -> SqlEngineAttributes:
        """Collection of attributes and features specific to the Snowflake SQL engine"""
        return DuckDbEngineAttributes()

    def cancel_submitted_queries(self) -> None:  # noqa: D
        raise NotImplementedError
