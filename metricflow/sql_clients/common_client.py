from typing import Optional, TypeVar

from metricflow.object_utils import ExtendedEnum


class SqlDialect(ExtendedEnum):
    """All SQL dialects that MQL currently supports. Value of enum is used in URLs as the dialect."""

    REDSHIFT = "redshift"
    POSTGRESQL = "postgresql"
    POSTGRES = "postgres"
    MYSQL = "mysql"
    SNOWFLAKE = "snowflake"
    SQLITE = "sqlite"
    BIGQUERY = "bigquery"


T = TypeVar("T")


def not_empty(value: Optional[T], component_name: str, url: str) -> T:
    """Helper to check the value is not None - otherwise raise a helpful exception."""
    if not value:
        raise ValueError(f"Missing {component_name} in {url}")
    else:
        return value
