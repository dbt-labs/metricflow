from __future__ import annotations

from abc import abstractmethod
from enum import Enum
from typing import ClassVar, Dict, Optional, Protocol, Sequence

from pandas import DataFrame

from metricflow.dataflow.sql_table import SqlTable
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql.sql_bind_parameters import SqlBindParameters


class SqlEngine(Enum):
    """Enumeration of SQL engines, including ones that are not yet supported."""

    BIGQUERY = "BigQuery"
    DUCKDB = "DuckDB"
    REDSHIFT = "Redshift"
    POSTGRES = "Postgres"
    SNOWFLAKE = "Snowflake"
    DATABRICKS = "Databricks"


class SqlIsolationLevel(Enum):
    """Describes the isolation levels used to execute SQL queries. Values are passed as options to SQLAlchemy."""

    READ_UNCOMMITTED = "READ_UNCOMMITTED"
    READ_COMMITTED = "READ_COMMITTED"
    REPEATABLE_READ = "REPEATABLE_READ"
    SNAPSHOT = "SNAPSHOT"
    # Unique to Databricks.
    WRITE_SERIALIZABLE = "WRITE_SERIALIZABLE"
    SERIALIZABLE = "SERIALIZABLE"


class SqlClient(Protocol):
    """Base interface for SqlClient instances used inside MetricFlow.

    This provides the methods needed to execute SQL queries against the relevant Data Warehouse configuration.
    """

    @property
    @abstractmethod
    def sql_engine_attributes(self) -> SqlEngineAttributes:
        """Return a struct of engine-specific metadata.

        See documentation on SqlEngineAttributes for details. This property is configured as a tagged
        method rather than a simple property because defining it as a simple property makes it settable,
        and we should not be re-using SqlClient instances with different SqlEngineAttributes.
        """
        raise NotImplementedError

    @abstractmethod
    def create_table_as_select(
        self,
        sql_table: SqlTable,
        select_query: str,
        sql_bind_parameters: SqlBindParameters = SqlBindParameters(),
    ) -> None:
        """Method for creating a table from the provided select query.

        Args:
            sql_table: The SqlTable metadata of the table to create
            select_query: The query to use to populate the table
            sql_bind_parameters: Map of values to substitute in to parameterized sql query strings
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
    ) -> DataFrame:
        """Base query method, upon execution will run a query that returns a pandas DataFrame."""
        raise NotImplementedError

    @abstractmethod
    def execute(
        self,
        stmt: str,
        sql_bind_parameters: SqlBindParameters = SqlBindParameters(),
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
    def health_checks(self, schema_name: str) -> Dict[str, Dict[str, str]]:
        """Run health checks against the underlying Data Warehouse.

        TODO: Consider restructuring this so the health checks are separate from the SqlClient
        TODO: Re-evaluate the return type to see if there's a more structured option available

        This method fires a bunch of queries and collects results inside of a flexible output object for
        later processing. This method has been ported over to the SqlClient Protocol from the original
        closed-source client classes. It was included there because certain health checks are warehouse-specific,
        e.g., there are checks to verify that specific session values are properly configured for Snowflake
        instances, and migrating to a new model will be quite messy in the short term. Longer term it likely
        makes sense for these to be in a separate module, but for the time being the CLI will invoke health
        checks and therefore this needs to remain part of the core Protocol.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:  # noqa: D
        """Close the connections / engines used by this client."""
        raise NotImplementedError

    @abstractmethod
    def cancel_submitted_queries(self) -> None:  # noqa: D
        """Cancel queries submitted through this client (that may be still running) with best-effort."""
        raise NotImplementedError

    @abstractmethod
    def render_bind_parameter_key(self, bind_parameter_key: str) -> str:
        """Wrap the bind parameter key with syntax accepted by engine."""
        raise NotImplementedError


class SqlEngineAttributes(Protocol):
    """Base interface for SQL engine-specific attributes and features.

    These include items like support for language features (e.g., FULL OUTER JOIN support), dialect differences
    (e.g., DOUBLE type name), and things of that nature.

    Concrete implementations would typically be the equivalent of frozen dataclass literals, one per
    MetricFlowSupportedDBEngine, as we would not expect these properties to change from one client to the next.

    Concrete implementations SHOULD NOT inherit from this protocol, as the typechecker may not catch issues
    caused by changes to the protocol itself when inheritance is used.
    """

    sql_engine_type: ClassVar[SqlEngine]

    # SQL Engine capabilities
    # The isolation levels supported as options through the SqlClient API. This may be a subset of the isolation levels
    # supported by the engine until implementation / testing is complete.
    supported_isolation_levels: ClassVar[Sequence[SqlIsolationLevel]]
    date_trunc_supported: ClassVar[bool]
    full_outer_joins_supported: ClassVar[bool]
    indexes_supported: ClassVar[bool]
    multi_threading_supported: ClassVar[bool]
    timestamp_type_supported: ClassVar[bool]
    timestamp_to_string_comparison_supported: ClassVar[bool]
    cancel_submitted_queries_supported: ClassVar[bool]
    continuous_percentile_aggregation_supported: ClassVar[bool]
    discrete_percentile_aggregation_supported: ClassVar[bool]
    approximate_continuous_percentile_aggregation_supported: ClassVar[bool]
    approximate_discrete_percentile_aggregation_supported: ClassVar[bool]

    # SQL Dialect replacement strings
    double_data_type_name: ClassVar[str]
    timestamp_type_name: ClassVar[Optional[str]]
    random_function_name: ClassVar[str]

    # MetricFlow attributes
    sql_query_plan_renderer: ClassVar[SqlQueryPlanRenderer]
