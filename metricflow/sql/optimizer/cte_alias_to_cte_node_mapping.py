from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Optional

from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

from metricflow.sql.sql_plan import SqlCteAliasMapping, SqlCteNode, SqlSelectStatementNode

logger = logging.getLogger(__name__)



class SqlCteAliasMappingLookup:
    def __init__(self) -> None:
        self._select_node_to_cte_alias_mapping: Dict[SqlSelectStatementNode, SqlCteAliasMapping] = {}

    def cte_alias_mapping_exists(self, select_node: SqlSelectStatementNode) -> bool:
        return select_node in self._select_node_to_cte_alias_mapping

    def add_cte_alias_mapping(
        self, select_node: SqlSelectStatementNode, cte_alias_mapping: SqlCteAliasMapping,
    ) -> None:
        if select_node in self._select_node_to_cte_alias_mapping:
            raise RuntimeError(
                str(
                    LazyFormat(
                        "`select_node` node has already been added,",
                        # child_select_node=child_select_node,
                        select_node=select_node,
                        current_mapping=self._select_node_to_cte_alias_mapping,
                    )
                )
            )

        self._select_node_to_cte_alias_mapping[select_node] = cte_alias_mapping

    def get_cte_alias_mapping(self, select_node: SqlSelectStatementNode) -> SqlCteAliasMapping:
        cte_alias_mapping = self._select_node_to_cte_alias_mapping.get(select_node)
        if cte_alias_mapping is None:
            raise RuntimeError(
                str(LazyFormat("CTE alias mapping does not exist for the given `select_node`", select_node=select_node))
            )
        return cte_alias_mapping
