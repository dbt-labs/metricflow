from __future__ import annotations

import logging
from collections import defaultdict
from typing import Dict, FrozenSet, Iterable, Set

from metricflow.sql.sql_plan import (
    SqlQueryPlanNode,
)

logger = logging.getLogger(__name__)


class NodeToColumnAliasMapping:
    """Mutable class for mapping a SQL node to an arbitrary set of column aliases for that node.

    * Alternatively, this can be described as mapping a location in the SQL query plan to a set of column aliases.
    * See `SqlMapRequiredColumnAliasesVisitor` for the main use case for this class.
    * This is a thin wrapper over a dict to aid readability.
    """

    def __init__(self) -> None:  # noqa: D107
        self._node_to_tagged_aliases: Dict[SqlQueryPlanNode, Set[str]] = defaultdict(set)

    def get_aliases(self, node: SqlQueryPlanNode) -> FrozenSet[str]:
        """Return the column aliases added for the given SQL node."""
        return frozenset(self._node_to_tagged_aliases[node])

    def add_alias(self, node: SqlQueryPlanNode, column_alias: str) -> None:  # noqa: D102
        return self._node_to_tagged_aliases[node].add(column_alias)

    def add_aliases(self, node: SqlQueryPlanNode, column_aliases: Iterable[str]) -> None:  # noqa: D102
        self._node_to_tagged_aliases[node].update(column_aliases)
