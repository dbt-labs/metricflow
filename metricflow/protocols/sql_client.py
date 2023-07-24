from __future__ import annotations

from abc import abstractmethod
from enum import Enum
from typing import Protocol

from pandas import DataFrame

from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql_request.sql_request_attributes import SqlJsonTag


class SqlEngine(Enum):
    """Enumeration of supported SQL engines.

    Values are normalized engine names used for things like snapshot file locations.
    """

    BIGQUERY = "BigQuery"
    DUCKDB = "DuckDB"
    REDSHIFT = "Redshift"
    POSTGRES = "Postgres"
    SNOWFLAKE = "Snowflake"
    DATABRICKS = "Databricks"


class SqlClient(Protocol):
    """Base interface for SqlClient instances used inside MetricFlow.

    This provides the methods needed to execute SQL queries against the relevant Data Warehouse configuration.
    """

    @property
    @abstractmethod
    def sql_engine_type(self) -> SqlEngine:
        """Enumerated value representing the underlying SqlEngine for this SqlClient instance."""
        raise NotImplementedError

    @property
    @abstractmethod
    def sql_query_plan_renderer(self) -> SqlQueryPlanRenderer:
        """Dialect-specific SQL query plan renderer used for converting MetricFlow's query plan to executable SQL.

        This is bundled with the SqlClient partly as a convenenience for accessing a single instance of the renderer,
        and partly due to the close relationship between dialect and engine capabilities.
        """
        raise NotImplementedError

    @abstractmethod
    def query(
        self,
        stmt: str,
        sql_bind_parameters: SqlBindParameters = SqlBindParameters(),
        extra_tags: SqlJsonTag = SqlJsonTag(),
    ) -> DataFrame:
        """Base query method, upon execution will run a query that returns a pandas DataFrame."""
        raise NotImplementedError

    @abstractmethod
    def execute(
        self,
        stmt: str,
        sql_bind_parameters: SqlBindParameters = SqlBindParameters(),
        extra_tags: SqlJsonTag = SqlJsonTag(),
    ) -> None:
        """Base execute method."""
        raise NotImplementedError

    @abstractmethod
    def dry_run(
        self,
        stmt: str,
        sql_bind_parameters: SqlBindParameters = SqlBindParameters(),
    ) -> None:
        """Base dry_run method."""
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:  # noqa: D
        """Close the connections / engines used by this client."""
        raise NotImplementedError

    @abstractmethod
    def render_bind_parameter_key(self, bind_parameter_key: str) -> str:
        """Wrap the bind parameter key with syntax accepted by engine."""
        raise NotImplementedError
