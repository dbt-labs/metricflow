from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple, Union


class SqlTableType(Enum):  # noqa: D101
    TABLE = "table"
    VIEW = "view"


@dataclass(frozen=True, order=True)
class SqlTable:
    """Represents a reference to a SQL table."""

    schema_name: str
    table_name: str
    db_name: Optional[str] = None
    table_type: SqlTableType = SqlTableType.TABLE

    @staticmethod
    def from_string(sql_str: str) -> SqlTable:  # noqa: D102
        sql_str_split = sql_str.split(".")
        if len(sql_str_split) == 2:
            return SqlTable(schema_name=sql_str_split[0], table_name=sql_str_split[1])
        elif len(sql_str_split) == 3:
            return SqlTable(db_name=sql_str_split[0], schema_name=sql_str_split[1], table_name=sql_str_split[2])
        raise RuntimeError(
            f"Invalid input for a SQL table, expected form '<schema>.<table>' or '<db>.<schema>.<table>' "
            f"but got: {sql_str}"
        )

    @property
    def sql(self) -> str:
        """Return the snippet that can be used for use in SQL queries."""
        if self.db_name:
            return f"{self.db_name}.{self.schema_name}.{self.table_name}"
        return f"{self.schema_name}.{self.table_name}"

    @property
    def parts_tuple(self) -> Union[Tuple[str, str], Tuple[str, str, str]]:
        """Return a tuple of the sql table parts."""
        if self.db_name:
            return (self.db_name, self.schema_name, self.table_name)
        else:
            return (self.schema_name, self.table_name)
