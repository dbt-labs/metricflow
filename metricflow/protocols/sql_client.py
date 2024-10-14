from __future__ import annotations

from abc import abstractmethod
from enum import Enum
from typing import Protocol, Set

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet

from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer


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
    TRINO = "Trino"

    @property
    def unsupported_granularities(self) -> Set[TimeGranularity]:
        """Granularities that can't be used with this SqlEngine.

        We allow the smallest granularity the SqlEngine supports for its base TIMESTAMP type and all our required
        operations (e.g., DATE_TRUNC). For example, when we added support for these granularities
        Trino supported more precise types for storage and access, but Trino's base TIMESTAMP type and
        DATE_TRUNC function only supported millisecond precision.
        """
        if self is SqlEngine.SNOWFLAKE:
            return set()
        elif self is SqlEngine.BIGQUERY:
            return {TimeGranularity.NANOSECOND}
        elif self is SqlEngine.DATABRICKS:
            return {TimeGranularity.NANOSECOND}
        elif self is SqlEngine.DUCKDB:
            return {TimeGranularity.NANOSECOND}
        elif self is SqlEngine.POSTGRES:
            return {TimeGranularity.NANOSECOND}
        elif self is SqlEngine.REDSHIFT:
            return {TimeGranularity.NANOSECOND}
        elif self is SqlEngine.TRINO:
            return {TimeGranularity.NANOSECOND, TimeGranularity.MICROSECOND}
        else:
            assert_values_exhausted(self)


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
        sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet(),
    ) -> MetricFlowDataTable:
        """Base query method, upon execution will run a query that returns a pandas DataTable."""
        raise NotImplementedError

    @abstractmethod
    def execute(
        self,
        stmt: str,
        sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet(),
    ) -> None:
        """Base execute method."""
        raise NotImplementedError

    @abstractmethod
    def dry_run(
        self,
        stmt: str,
        sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet(),
    ) -> None:
        """Base dry_run method."""
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """Close the connections / engines used by this client."""
        raise NotImplementedError

    @abstractmethod
    def render_bind_parameter_key(self, bind_parameter_key: str) -> str:
        """Wrap the bind parameter key with syntax accepted by engine."""
        raise NotImplementedError
