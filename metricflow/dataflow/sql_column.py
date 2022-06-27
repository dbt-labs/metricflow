from __future__ import annotations
from enum import Enum
from typing import Optional

from pandas.api.types import (
    is_datetime64_any_dtype as is_datetime_dtype,
    is_float_dtype,
    is_integer_dtype,
    is_bool_dtype,
    is_string_dtype,
)

from metricflow.dataflow.sql_table import SqlTable
from metricflow.model.objects.utils import FrozenBaseModel, ParseableField


class SqlColumnType(str, Enum):
    """Represents a column type."""

    STRING = "string"
    BOOLEAN = "boolean"
    INTEGER = "integer"
    FLOAT = "float"
    DATETIME = "datetime"
    UNKNOWN = "unknown"

    @staticmethod
    def from_pandas_dtype(dtype: Optional[str]) -> SqlColumnType:
        """Get the column type from a pd.Series.dtype"""
        if is_integer_dtype(dtype):
            return SqlColumnType.INTEGER
        if is_float_dtype(dtype):
            return SqlColumnType.FLOAT
        if is_datetime_dtype(dtype):
            return SqlColumnType.DATETIME
        if is_bool_dtype(dtype):
            return SqlColumnType.BOOLEAN
        if is_string_dtype(dtype):
            return SqlColumnType.STRING
        return SqlColumnType.UNKNOWN


class SqlColumn(FrozenBaseModel, ParseableField):
    """Represents a reference to a SQL column."""

    table: SqlTable
    name: str

    @staticmethod
    def parse(s: str) -> SqlColumn:
        """Implement ParseableField interface"""
        return SqlColumn.from_string(s)

    @staticmethod
    def from_string(sql_str: str) -> SqlColumn:  # noqa: D
        table_str, column_name = sql_str.rsplit(".", 1)
        table = SqlTable.parse(table_str)
        return SqlColumn(table=table, name=column_name)

    @property
    def sql(self) -> str:
        """Return the snippet that can be used for use in SQL queries."""
        return f"{self.table.sql}.{self.name}"
