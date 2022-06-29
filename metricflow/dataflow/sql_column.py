from __future__ import annotations
from enum import Enum
from typing import Optional

from metricflow.dataflow.sql_table import SqlTable
from metricflow.model.objects.utils import FrozenBaseModel


class SqlColumnType(str, Enum):
    """Represents a column type."""

    STRING = "string"
    BOOLEAN = "boolean"
    INTEGER = "integer"
    FLOAT = "float"
    DATETIME = "datetime"
    UNKNOWN = "unknown"


class SqlColumn(FrozenBaseModel):
    """Represents a reference to a SQL column."""

    table: SqlTable
    column_name: str

    def __init__(  # noqa: D
        self,
        column_name: str,
        table_name: str = None,
        schema_name: str = None,
        db_name: str = None,
        table: SqlTable = None,
    ):
        if table is None:
            table = SqlTable(db_name=db_name, schema_name=schema_name, table_name=table_name)

        super(FrozenBaseModel, self).__init__(table=table, column_name=column_name)

    @staticmethod
    def from_string(sql_str: str) -> SqlColumn:  # noqa: D
        table_str, column_name = sql_str.rsplit(".", 1)
        table = SqlTable.from_string(table_str)
        return SqlColumn(table=table, column_name=column_name)

    @property
    def db_name(self) -> Optional[str]:  # noqa: D
        return self.table.db_name

    @property
    def schema_name(self) -> Optional[str]:  # noqa: D
        return self.table.schema_name

    @property
    def table_name(self) -> Optional[str]:  # noqa: D
        return self.table.table_name

    @property
    def sql(self) -> str:
        """Return the snippet that can be used for use in SQL queries."""
        return f"{self.table.sql}.{self.column_name}"

    def __repr__(self) -> str:  # noqa: D
        return f"SqlColumn(full_name={self.sql})"
