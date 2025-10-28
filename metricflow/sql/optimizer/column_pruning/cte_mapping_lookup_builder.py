from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

from metricflow.sql.optimizer.column_pruning.cte_alias_to_cte_node_mapping import SqlCteAliasMappingLookup
from metricflow.sql.sql_ctas_node import SqlCreateTableAsNode
from metricflow.sql.sql_cte_node import SqlCteAliasMapping, SqlCteNode
from metricflow.sql.sql_plan import (
    SqlPlanNode,
    SqlPlanNodeVisitor,
)
from metricflow.sql.sql_select_node import SqlSelectStatementNode
from metricflow.sql.sql_select_text_node import SqlSelectTextNode
from metricflow.sql.sql_table_node import SqlTableNode

logger = logging.getLogger(__name__)


class SqlCteAliasMappingLookupBuilderVisitor(SqlPlanNodeVisitor[None]):
    """Traverses the SQL plan and builds the associated `SqlCteAliasMappingLookup`.

    Please see `SqlCteAliasMappingLookup` for more details.
    """

    def __init__(self) -> None:  # noqa: D107
        self._current_cte_alias_mapping = SqlCteAliasMapping()
        self._cte_alias_mapping_lookup = SqlCteAliasMappingLookup()

    @contextmanager
    def _save_current_cte_alias_mapping(self) -> Iterator[None]:
        previous_cte_alias_mapping = self._current_cte_alias_mapping
        yield
        self._current_cte_alias_mapping = previous_cte_alias_mapping

    def _default_handler(self, node: SqlPlanNode) -> None:
        """Default recursive handler to visit the parents of the given node."""
        for parent_node in node.parent_nodes:
            with self._save_current_cte_alias_mapping():
                parent_node.accept(self)
        return

    @override
    def visit_cte_node(self, node: SqlCteNode) -> None:
        return self._default_handler(node)

    @override
    def visit_select_statement_node(self, node: SqlSelectStatementNode) -> None:
        """Based on required column aliases for this SELECT, figure out required column aliases in parents."""
        logger.debug(
            LazyFormat(
                "Starting visit of SELECT statement node with CTE alias mapping",
                node=node,
                current_cte_alias_mapping=self._current_cte_alias_mapping,
            )
        )

        if self._cte_alias_mapping_lookup.cte_alias_mapping_exists(node):
            return self._default_handler(node)

        # Record that can see the CTEs defined in this SELECT node and outer SELECT statements.
        # CTEs defined in this select node should override ones that were defined in the outer SELECT in case
        # of CTE alias collisions.
        self._current_cte_alias_mapping = self._current_cte_alias_mapping.merge(
            SqlCteAliasMapping.create({cte_node.cte_alias: cte_node for cte_node in node.cte_sources})
        )
        self._cte_alias_mapping_lookup.add_cte_alias_mapping(
            select_node=node,
            cte_alias_mapping=self._current_cte_alias_mapping,
        )

        return self._default_handler(node)

    @override
    def visit_table_node(self, node: SqlTableNode) -> None:
        self._default_handler(node)

    @override
    def visit_query_from_clause_node(self, node: SqlSelectTextNode) -> None:
        self._default_handler(node)

    @override
    def visit_create_table_as_node(self, node: SqlCreateTableAsNode) -> None:  # noqa: D102
        self._default_handler(node)

    @property
    def cte_alias_mapping_lookup(self) -> SqlCteAliasMappingLookup:
        """Returns the lookup created after traversal."""
        return self._cte_alias_mapping_lookup
