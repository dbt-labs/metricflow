from __future__ import annotations

import logging
from typing import Dict

from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from metricflow.sql.sql_cte_node import SqlCteAliasMapping
from metricflow.sql.sql_select_node import SqlSelectStatementNode

logger = logging.getLogger(__name__)


class SqlCteAliasMappingLookup:
    """A mutable lookup that stores the CTE-alias mapping at a given node.

    In cases with nested CTEs in a SELECT, it's possible that a CTE defined in an inner SELECT has an alias that is the
    same as a CTE defined in an outer SELECT. e.g.

        # outer_cte
        WITH cte_0 AS (
            SELECT 1 AS col_0
        )

        # outer_select
        SELECT col_0
        FROM (

            # inner_cte
            WITH cte_0 AS (
                SELECT 2 AS col_0
            )
            # inner_select
            SELECT col_0 FROM cte_0
        )
        ...

    In this case, `outer_cte` and `inner_cte` both have the same alias `cte_0`. When `cte_0` is referenced from
    `inner_select`, it is referring to the `inner_cte`. For column pruning, it is necessary to figure out which CTE
    a given alias is referencing, so this class helps to keep track of that mapping.
    """

    def __init__(self) -> None:  # noqa: D107
        self._select_node_to_cte_alias_mapping: Dict[SqlSelectStatementNode, SqlCteAliasMapping] = {}

    def cte_alias_mapping_exists(self, select_node: SqlSelectStatementNode) -> bool:
        """Returns true if the CTE-alias mapping for the given node has been recorded."""
        return select_node in self._select_node_to_cte_alias_mapping

    def add_cte_alias_mapping(
        self,
        select_node: SqlSelectStatementNode,
        cte_alias_mapping: SqlCteAliasMapping,
    ) -> None:
        """Associate the given CTE-alias mapping with the given node.

        Raises an exception if a mapping already exists.
        """
        if select_node in self._select_node_to_cte_alias_mapping:
            raise RuntimeError(
                LazyFormat(
                    "`select_node` node has already been added,",
                    # child_select_node=child_select_node,
                    select_node=select_node,
                    current_mapping=self._select_node_to_cte_alias_mapping,
                )
            )

        self._select_node_to_cte_alias_mapping[select_node] = cte_alias_mapping

    def get_cte_alias_mapping(self, select_node: SqlSelectStatementNode) -> SqlCteAliasMapping:
        """Return the CTE-alias mapping for the given node.

        Raises an exception if a mapping was not previously added for the given node.
        """
        cte_alias_mapping = self._select_node_to_cte_alias_mapping.get(select_node)
        if cte_alias_mapping is None:
            raise RuntimeError(
                LazyFormat("CTE alias mapping does not exist for the given `select_node`", select_node=select_node)
            )

        return cte_alias_mapping
