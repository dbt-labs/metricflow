from __future__ import annotations

from abc import abstractmethod
from typing import Optional, Protocol

from pandas import DataFrame

from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SqlClient


class SqlClientWithDDLMethods(SqlClient, Protocol):
    """SqlClient protocol with DDL methods enabled.

    The core MetricFlow SqlClient is effectively a read-only construct, as MetricFlow is meant to compile and run SQL
    for metric queries, not manage downstream database state.

    However, it can be useful to have a SqlClient that can do DDL. In this case, we use these methods in our warehouse
    engine integration tests for setup and teardown. All of the SqlClients we use in our tests must implement this
    protocol, or else the integration tests cannot be run.
    """

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
    def create_schema(self, schema_name: str) -> None:
        """Create the given schema if it doesn't already exist."""
        raise NotImplementedError

    @abstractmethod
    def drop_schema(self, schema_name: str, cascade: bool) -> None:  # noqa: D
        """Drop the given schema if it exists. If cascade is set, drop the tables in the schema first."""
        raise NotImplementedError
