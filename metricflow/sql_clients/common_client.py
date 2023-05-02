from typing import Optional, TypeVar

from dbt_semantic_interfaces.enum_extension import ExtendedEnum
from metricflow.protocols.sql_client import SqlClient, SqlIsolationLevel


class SqlDialect(ExtendedEnum):
    """All SQL dialects that MQL currently supports. Value of enum is used in URLs as the dialect."""

    DUCKDB = "duckdb"
    REDSHIFT = "redshift"
    POSTGRESQL = "postgresql"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    DATABRICKS = "databricks"


T = TypeVar("T")


def not_empty(value: Optional[T], component_name: str, url: str) -> T:
    """Helper to check the value is not None - otherwise raise a helpful exception."""
    if not value:
        raise ValueError(f"Missing {component_name} in {url}")
    else:
        return value


def check_isolation_level(sql_client: SqlClient, isolation_level: Optional[SqlIsolationLevel] = None) -> None:
    """Throws an exception if the isolation level is not supported by the engine."""
    if (
        isolation_level is not None
        and isolation_level not in sql_client.sql_engine_attributes.supported_isolation_levels
    ):
        raise NotImplementedError(f"Isolation level not yet supported: {isolation_level}")
