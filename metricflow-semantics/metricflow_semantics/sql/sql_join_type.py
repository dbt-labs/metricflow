from __future__ import annotations

from enum import Enum


class SqlJoinType(Enum):
    """Enumerates the different kinds of SQL joins.

    The value is the SQL string to be used when rendering the join.
    """

    LEFT_OUTER = "LEFT OUTER JOIN"
    FULL_OUTER = "FULL OUTER JOIN"
    INNER = "INNER JOIN"
    CROSS_JOIN = "CROSS JOIN"

    def __repr__(self) -> str:  # noqa: D105
        return f"{self.__class__.__name__}.{self.name}"
