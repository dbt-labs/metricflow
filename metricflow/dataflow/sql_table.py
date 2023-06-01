from __future__ import annotations

from typing import Optional, Tuple, Union

from dbt_semantic_interfaces.implementations.base import (
    FrozenBaseModel,
    PydanticCustomInputParser,
    PydanticParseableValueType,
)


class SqlTable(PydanticCustomInputParser, FrozenBaseModel):
    """Represents a reference to a SQL table."""

    db_name: Optional[str] = None
    schema_name: str
    table_name: str

    @classmethod
    def _from_yaml_value(cls, input: PydanticParseableValueType) -> SqlTable:
        """Parses a SqlTable from string input found in a user-provided model specification.

        Raises a ValueError on any non-string input, as all user-provided specifications of table entities
        should be strings conforming to the expectations defined in the from_string method.
        """
        if isinstance(input, str):
            return SqlTable.from_string(input)
        else:
            raise ValueError(
                f"SqlTable inputs from model configs are expected to always be of type string, but got type "
                f"{type(input)} with value: {input}"
            )

    @staticmethod
    def from_string(sql_str: str) -> SqlTable:  # noqa: D
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
