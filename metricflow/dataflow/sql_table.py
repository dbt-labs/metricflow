from __future__ import annotations
from typing import Optional

from metricflow.model.objects.utils import FrozenBaseModel, ParseableField


class SqlTable(FrozenBaseModel, ParseableField):
    """Represents a reference to a SQL table."""

    db_name: Optional[str] = None
    schema_name: str
    table_name: str

    @staticmethod
    def parse(s: str) -> SqlTable:
        """Implement ParseableField interface"""
        return SqlTable.from_string(s)

    @staticmethod
    def from_string(sql_str: str) -> SqlTable:  # noqa: D
        sql_str_split = sql_str.split(".")
        if len(sql_str_split) == 2:
            return SqlTable(schema_name=sql_str_split[0], table_name=sql_str_split[1])
        elif len(sql_str_split) == 3:
            return SqlTable(db_name=sql_str_split[0], schema_name=sql_str_split[1], table_name=sql_str_split[2])
        raise RuntimeError(f"Invalid input for a SQL table, expected form '<schema>.<table>' but got: {sql_str}")

    @property
    def sql(self) -> str:
        """Return the snippet that can be used for use in SQL queries."""
        if self.db_name:
            return f"{self.db_name}.{self.schema_name}.{self.table_name}"
        return f"{self.schema_name}.{self.table_name}"
