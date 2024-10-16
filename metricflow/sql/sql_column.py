from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from metricflow_semantics.sql.sql_table import SqlTable


@dataclass(frozen=True, order=True)
class SqlColumn:
    """Represents a reference to a SQL column."""

    table: SqlTable
    column_name: str

    @staticmethod
    def from_names(
        column_name: str,
        table_name: str,
        schema_name: str,
        db_name: str,
    ) -> SqlColumn:
        """Helper factory method for constructing a column from database, table, schema and column names."""
        table = SqlTable(db_name=db_name, schema_name=schema_name, table_name=table_name)
        return SqlColumn(table=table, column_name=column_name)

    @staticmethod
    def from_string(sql_str: str) -> SqlColumn:  # noqa: D102
        table_str, column_name = sql_str.rsplit(".", 1)
        table = SqlTable.from_string(table_str)
        return SqlColumn(table=table, column_name=column_name)

    @property
    def db_name(self) -> Optional[str]:  # noqa: D102
        return self.table.db_name

    @property
    def schema_name(self) -> Optional[str]:  # noqa: D102
        return self.table.schema_name

    @property
    def table_name(self) -> str:  # noqa: D102
        return self.table.table_name

    @property
    def sql(self) -> str:
        """Return the snippet that can be used for use in SQL queries."""
        return f"{self.table.sql}.{self.column_name}"

    def __repr__(self) -> str:  # noqa: D105
        return f"SqlColumn(full_name={self.sql})"
