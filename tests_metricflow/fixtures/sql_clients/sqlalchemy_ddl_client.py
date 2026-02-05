from __future__ import annotations

import datetime
import logging
import time
from typing import Optional

from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from sqlalchemy import text as sa_text

from metricflow.data_table.mf_column import ColumnDescription
from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.protocols.sql_client import SqlEngine
from tests_metricflow.fixtures.sql_clients.sqlalchemy_client import SqlAlchemyBasedSqlClient

logger = logging.getLogger(__name__)


class SqlAlchemyDDLSqlClient(SqlAlchemyBasedSqlClient):
    """SqlAlchemy-based client with DDL methods for test fixtures."""

    def create_table_from_data_table(
        self,
        sql_table: SqlTable,
        df: MetricFlowDataTable,
        chunk_size: Optional[int] = None,
    ) -> None:
        """Create table and populate with data.

        Strategy:
        1. Create table with typed columns
        2. Insert data in chunks using executemany for efficiency
        """
        logger.debug(LazyFormat(lambda: f"Creating table '{sql_table.sql}' from DataTable with {df.row_count} row(s)"))
        start_time = time.perf_counter()

        # Build CREATE TABLE statement
        column_defs = []
        for col_desc in df.column_descriptions:
            sql_type = self._get_sql_type(col_desc)
            column_defs.append(f"{col_desc.column_name} {sql_type}")

        create_stmt = f"CREATE TABLE IF NOT EXISTS {sql_table.sql} ({', '.join(column_defs)})"

        # Execute CREATE TABLE
        self.execute(create_stmt)

        # Insert data
        if df.row_count > 0:
            self._insert_data_into_table(sql_table, df, chunk_size)

        logger.debug(LazyFormat(lambda: f"Created table '{sql_table.sql}' in {time.perf_counter() - start_time:.2f}s"))

    def _insert_data_into_table(
        self,
        sql_table: SqlTable,
        df: MetricFlowDataTable,
        chunk_size: Optional[int] = None,
    ) -> None:
        """Insert data using batch INSERT statements."""
        chunk_size = chunk_size or 1000

        with self._engine.connect() as conn:
            values_list = []

            for row in df.rows:
                cells = []
                for cell in row:
                    if cell is None:
                        cells.append("null")
                    elif isinstance(cell, str):
                        # Escape and quote strings
                        escaped = self._quote_escape_value(str(cell))
                        cells.append(f"'{escaped}'")
                    elif isinstance(cell, datetime.datetime):
                        # Handle timestamps with engine-specific formatting
                        escaped = self._quote_escape_value(str(cell))
                        if self.sql_engine_type is SqlEngine.TRINO:
                            cells.append(f"timestamp '{escaped}'")
                        else:
                            cells.append(f"'{escaped}'")
                    else:
                        cells.append(str(cell))

                values_list.append(f"({', '.join(cells)})")

                # Insert in chunks
                if len(values_list) >= chunk_size:
                    values_str = ",\n".join(values_list)
                    conn.execute(sa_text(f"INSERT INTO {sql_table.sql} VALUES {values_str}"))
                    values_list = []

            # Insert remaining rows
            if values_list:
                values_str = ",\n".join(values_list)
                conn.execute(sa_text(f"INSERT INTO {sql_table.sql} VALUES {values_str}"))

            # Commit all inserts
            conn.commit()

    def _get_sql_type(self, column_description: ColumnDescription) -> str:
        """Get engine-specific SQL type for a column."""
        column_type = column_description.column_type

        if column_type is str:
            if self.sql_engine_type in (SqlEngine.DATABRICKS, SqlEngine.BIGQUERY):
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
            raise ValueError(f"Unexpected column type: {column_type}")

    def _quote_escape_value(self, value: str) -> str:
        """Escape quotes in string values."""
        if self.sql_engine_type in (SqlEngine.DATABRICKS, SqlEngine.BIGQUERY):
            return value.replace("'", "\\'")
        return value.replace("'", "''")

    def create_schema(self, schema_name: str) -> None:
        """Create schema if it doesn't exist."""
        self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    def drop_schema(self, schema_name: str, cascade: bool = True) -> None:
        """Drop schema if it exists."""
        cascade_clause = " CASCADE" if cascade else ""
        self.execute(f"DROP SCHEMA IF EXISTS {schema_name}{cascade_clause}")
