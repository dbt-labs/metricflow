from __future__ import annotations

import datetime
import logging
import time
from typing import Optional

from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import AdapterBackedSqlClient
from metricflow.data_table.mf_column import ColumnDescription
from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.protocols.sql_client import SqlEngine

logger = logging.getLogger(__name__)


class AdapterBackedDDLSqlClient(AdapterBackedSqlClient):
    """Extends the AdapterBackedSqlClient with the DDL methods necessary for test configuration and execution."""

    def create_table_from_data_table(
        self,
        sql_table: SqlTable,
        df: MetricFlowDataTable,
        chunk_size: Optional[int] = None,
    ) -> None:
        """Create a table in the data warehouse containing the contents of the data_table.

        Only used in tutorials and tests.

        Args:
            sql_table: The SqlTable object representing the table location to use
            df: The Pandas DataTable object containing the column schema and data to load
            chunk_size: The number of rows to insert per transaction
        """
        logger.debug(
            LazyFormat(lambda: f"Creating table '{sql_table.sql}' from a DataTable with {df.row_count} row(s)")
        )
        start_time = time.perf_counter()

        with self._adapter.connection_named("MetricFlow_create_from_dataframe"):
            # Create table
            columns_to_insert = []
            for column_description in df.column_descriptions:
                # Format as "column_name column_type"
                columns_to_insert.append(f"{column_description.column_name} {self._get_sql_type(column_description)}")

            self._adapter.execute(
                f"CREATE TABLE IF NOT EXISTS {sql_table.sql} ({', '.join(columns_to_insert)})",
                auto_begin=True,
                fetch=False,
            )
            self._adapter.commit_if_has_connection()

            # Insert rows
            values = []
            for row in df.rows:
                cells = []
                for cell in row:
                    if cell is None:
                        # use null keyword instead of isNA/None/etc.
                        cells.append("null")
                    elif type(cell) in [str, datetime.datetime]:
                        # Wrap cell in quotes & escape existing single quotes
                        escaped_cell = self._quote_escape_value(str(cell))
                        # Trino requires timestamp literals to be wrapped in a timestamp() function.
                        # There is probably a better way to handle this.
                        if self.sql_engine_type is SqlEngine.TRINO and type(cell) is datetime.datetime:
                            cells.append(f"timestamp '{escaped_cell}'")
                        else:
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

        logger.debug(
            LazyFormat(
                lambda: f"Created SQL table '{sql_table.sql}' from an in-memory table in {time.perf_counter() - start_time:.2f}s"
            )
        )

    def _get_sql_type(self, column_description: ColumnDescription) -> str:
        """Helper method to get the engine-specific type value.

        The dtype dict here is non-exhaustive but should be adequate for our needs.
        """
        # TODO: add type handling for string/bool/bigint types for all engines
        column_type = column_description.column_type
        if column_type is str:
            if self.sql_engine_type is SqlEngine.DATABRICKS or self.sql_engine_type is SqlEngine.BIGQUERY:
                return "string"
            if self.sql_engine_type is SqlEngine.TRINO:
                return "varchar"
            return "text"
        elif column_type is bool:
            return "boolean"
        elif column_type is int:
            return "bigint"
        elif column_type is float:
            return self._sql_plan_renderer.expr_renderer.double_data_type
        elif column_type is datetime.datetime:
            return self._sql_plan_renderer.expr_renderer.timestamp_data_type
        else:
            raise ValueError(f"Encountered unexpected {column_type=}!")

    def _quote_escape_value(self, value: str) -> str:
        """Escape single quotes in string-like values for create_table_from_data_table.

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
