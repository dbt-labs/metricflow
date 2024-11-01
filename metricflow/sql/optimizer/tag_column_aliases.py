from __future__ import annotations

import logging
from collections import defaultdict
from typing import Dict, FrozenSet, Iterable, Mapping, Set

from typing_extensions import override

from metricflow.sql.sql_plan import (
    SqlCreateTableAsNode,
    SqlCteNode,
    SqlQueryPlanNode,
    SqlQueryPlanNodeVisitor,
    SqlSelectQueryFromClauseNode,
    SqlSelectStatementNode,
    SqlTableNode,
)

logger = logging.getLogger(__name__)


class TaggedColumnAliasSet:
    """Keep track of column aliases in SELECT statements that have been tagged.

    The main use case for this class is to keep track of column aliases / columns that are required so that unnecessary
    columns can be pruned.

    For example, in this query:

        SELECT source_0.col_0 AS col_0
        FROM (
            SELECT
                example_table.col_0
                example_table.col_1
            FROM example_table
        ) source_0

    this class can be used to tag `example_table.col_0` but not tag `example_table.col_1` since it's not needed for the
    query to run correctly.
    """

    def __init__(self) -> None:  # noqa: D107
        self._node_to_tagged_aliases: Dict[SqlQueryPlanNode, Set[str]] = defaultdict(set)

    def get_tagged_aliases(self, node: SqlQueryPlanNode) -> FrozenSet[str]:
        """Return the given tagged column aliases associated with the given SQL node."""
        return frozenset(self._node_to_tagged_aliases[node])

    def tag_alias(self, node: SqlQueryPlanNode, column_alias: str) -> None:  # noqa: D102
        return self._node_to_tagged_aliases[node].add(column_alias)

    def tag_aliases(self, node: SqlQueryPlanNode, column_aliases: Iterable[str]) -> None:  # noqa: D102
        self._node_to_tagged_aliases[node].update(column_aliases)

    def tag_all_aliases_in_node(self, node: SqlQueryPlanNode) -> None:
        """Convenience method that tags all column aliases in the given SQL node, where appropriate."""
        node.accept(_TagAllColumnAliasesInNodeVisitor(self))

    def get_mapping(self) -> Mapping[SqlQueryPlanNode, FrozenSet[str]]:
        """Return mapping view that associates a given SQL node with the tagged column aliases in that node."""
        return {node: frozenset(tagged_aliases) for node, tagged_aliases in self._node_to_tagged_aliases.items()}


class _TagAllColumnAliasesInNodeVisitor(SqlQueryPlanNodeVisitor[None]):
    """Visitor to help implement `TaggedColumnAliasSet.tag_all_aliases_in_node`."""

    def __init__(self, required_column_alias_collector: TaggedColumnAliasSet) -> None:
        self._required_column_alias_collector = required_column_alias_collector

    @override
    def visit_select_statement_node(self, node: SqlSelectStatementNode) -> None:
        for select_column in node.select_columns:
            self._required_column_alias_collector.tag_alias(
                node=node,
                column_alias=select_column.column_alias,
            )

    @override
    def visit_table_node(self, node: SqlTableNode) -> None:
        """Columns in a SQL table are not represented."""
        pass

    @override
    def visit_query_from_clause_node(self, node: SqlSelectQueryFromClauseNode) -> None:
        """Columns in an arbitrary SQL query are not represented."""
        pass

    @override
    def visit_create_table_as_node(self, node: SqlCreateTableAsNode) -> None:
        for parent_node in node.parent_nodes:
            parent_node.accept(self)

    @override
    def visit_cte_node(self, node: SqlCteNode) -> None:
        for parent_node in node.parent_nodes:
            parent_node.accept(self)
