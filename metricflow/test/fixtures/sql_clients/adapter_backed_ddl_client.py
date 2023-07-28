from __future__ import annotations

import logging
import time
from typing import Optional

import pandas as pd

from metricflow.cli.dbt_connectors.adapter_backed_client import AdapterBackedSqlClient
from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SqlEngine

logger = logging.getLogger(__name__)


class AdapterBackedDDLSqlClient(AdapterBackedSqlClient):
    """Extends the AdapterBackedSqlClient with the DDL methods necessary for test configuration and execution."""

    def create_table_from_dataframe(
        self,
        sql_table: SqlTable,
        df: pd.DataFrame,
        chunk_size: Optional[int] = None,
    ) -> None:
        """Create a table in the data warehouse containing the contents of the dataframe.

        Only used in tutorials and tests.

        Args:
            sql_table: The SqlTable object representing the table location to use
            df: The Pandas DataFrame object containing the column schema and data to load
            chunk_size: The number of rows to insert per transaction
        """
        logger.info(f"Creating table '{sql_table.sql}' from a DataFrame with {df.shape[0]} row(s)")
        start_time = time.time()
        with self._adapter.connection_named("MetricFlow_create_from_dataframe"):
            # Create table
            # update dtypes to convert None to NA in boolean columns.
            # This mirrors the SQLAlchemy schema detection logic in pandas.io.sql
            df = df.convert_dtypes()
            columns = df.columns
            columns_to_insert = []
            for i in range(len(df.columns)):
                # Format as "column_name column_type"
                columns_to_insert.append(
                    f"{columns[i]} {self._get_type_from_pandas_dtype(str(df[columns[i]].dtype).lower())}"
                )
            self._adapter.execute(
                f"CREATE TABLE IF NOT EXISTS {sql_table.sql} ({', '.join(columns_to_insert)})",
                auto_begin=True,
                fetch=False,
            )
            self._adapter.commit_if_has_connection()

            # Insert rows
            values = []
            for row in df.itertuples(index=False, name=None):
                cells = []
                for cell in row:
                    if pd.isnull(cell):
                        # use null keyword instead of isNA/None/etc.
                        cells.append("null")
                    elif type(cell) in [str, pd.Timestamp]:
                        # Wrap cell in quotes & escape existing single quotes
                        escaped_cell = self._quote_escape_value(str(cell))
                        cells.append(f"'{escaped_cell}'")
                    else:
                        cells.append(str(cell))

                values.append(f"({', '.join(cells)})")
                if chunk_size and len(values) == chunk_size:
                    value_string = ",\n".join(values)
                    self._adapter.execute(
                        f"INSERT INTO {sql_table.sql} VALUES {value_string}", auto_begin=True, fetch=False
                    )
                    values = []
            if values:
                value_string = ",\n".join(values)
                self._adapter.execute(
                    f"INSERT INTO {sql_table.sql} VALUES {value_string}", auto_begin=True, fetch=False
                )
            # Commit all insert transaction at once
            self._adapter.commit_if_has_connection()

        logger.info(f"Created table '{sql_table.sql}' from a DataFrame in {time.time() - start_time:.2f}s")

    def _get_type_from_pandas_dtype(self, dtype: str) -> str:
        """Helper method to get the engine-specific type value.

        The dtype dict here is non-exhaustive but should be adequate for our needs.
        """
        # TODO: add type handling for string/bool/bigint types for all engines
        if dtype == "string" or dtype == "object":
            if self.sql_engine_type is SqlEngine.DATABRICKS or self.sql_engine_type is SqlEngine.BIGQUERY:
                return "string"
            return "text"
        elif dtype == "boolean" or dtype == "bool":
            return "boolean"
        elif dtype == "int64":
            return "bigint"
        elif dtype == "float64":
            return self._sql_query_plan_renderer.expr_renderer.double_data_type
        elif dtype == "datetime64[ns]":
            return self._sql_query_plan_renderer.expr_renderer.timestamp_data_type
        else:
            raise ValueError(f"Encountered unexpected Pandas dtype ({dtype})!")

    def _quote_escape_value(self, value: str) -> str:
        """Escape single quotes in string-like values for create_table_from_dataframe.

        This is necessary because Databricks uses backslash as its escape character.
        We don't bother with the exhaustive switch here because we expect most engines
        to fit into the default single quote condition.
        """
        if self.sql_engine_type is SqlEngine.DATABRICKS or self.sql_engine_type is SqlEngine.BIGQUERY:
            return value.replace("'", "\\'")

        return value.replace("'", "''")

    def create_schema(self, schema_name: str) -> None:
        """Create the given schema in a data warehouse. Only used in tutorials and tests."""
        self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    def drop_schema(self, schema_name: str, cascade: bool = True) -> None:
        """Drop the given schema from the data warehouse. Only used in tests."""
        self.execute(f"DROP SCHEMA IF EXISTS {schema_name}{' CASCADE' if cascade else ''}")
