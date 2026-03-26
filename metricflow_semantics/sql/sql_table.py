from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from dbt_semantic_interfaces.protocols.node_relation import NodeRelation


class SqlTableType(Enum):  # noqa: D101
    TABLE = "table"
    VIEW = "view"
    # CTE type may be added later.


@dataclass(frozen=True, order=True)
class SqlTable:
    """Represents a reference to a SQL table."""

    schema_name: Optional[str]
    table_name: str
    db_name: Optional[str] = None
    table_type: SqlTableType = SqlTableType.TABLE

    def __post_init__(self) -> None:  # noqa: D105
        if self.db_name is not None and self.schema_name is None:
            raise ValueError(f"{self.db_name=} when it should be specified with {self.schema_name=}")

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

    @staticmethod
    def from_node_relation(node_relation: NodeRelation) -> SqlTable:
        """Create a SQL table from a node relation."""
        return SqlTable.from_string(node_relation.relation_name)

    @property
    def sql(self) -> str:
        """Return the snippet that can be used for use in SQL queries."""
        items = []
        if self.db_name is not None:
            items.append(self.db_name)
        if self.schema_name is not None:
            items.append(self.schema_name)
        items.append(self.table_name)

        return ".".join(items)
