from __future__ import annotations

from abc import abstractmethod
from enum import Enum
from typing import Optional, Protocol, Sequence

from pandas import DataFrame

from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_request import SqlJsonTag
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql.sql_bind_parameters import SqlBindParameters


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
    def create_table_from_dataframe(
        self,
        sql_table: SqlTable,
        df: DataFrame,
        chunk_size: Optional[int] = None,
    ) -> None:
        """Creates a table and populates it with the contents of the dataframe.

        Args:
            sql_table: The SqlTable metadata of the table to create
            df: The Pandas DataFrame with the contents of the target table
            chunk_size: The number of rows to write per query
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
    def list_tables(self, schema_name: str) -> Sequence[str]:
        """List the tables in the given schema."""
        raise NotImplementedError

    @abstractmethod
    def table_exists(self, sql_table: SqlTable) -> bool:
        """Determines whether or not the given table exists in the data warehouse."""
        raise NotImplementedError

    @abstractmethod
    def drop_table(self, sql_table: SqlTable) -> None:
        """Drop the given table from the data warehouse."""
        raise NotImplementedError

    @abstractmethod
    def create_schema(self, schema_name: str) -> None:
        """Create the given schema if it doesn't already exist."""
        raise NotImplementedError

    @abstractmethod
    def drop_schema(self, schema_name: str, cascade: bool) -> None:  # noqa: D
        """Drop the given schema if it exists. If cascade is set, drop the tables in the schema first."""
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:  # noqa: D
        """Close the connections / engines used by this client."""
        raise NotImplementedError

    @abstractmethod
    def render_bind_parameter_key(self, bind_parameter_key: str) -> str:
        """Wrap the bind parameter key with syntax accepted by engine."""
        raise NotImplementedError
