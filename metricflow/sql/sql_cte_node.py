from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Mapping, Optional, Sequence, Tuple

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.toolkit.merger import Mergeable
from metricflow_semantics.toolkit.visitor import VisitorOutputT
from typing_extensions import override

from metricflow.sql.sql_plan import SqlPlanNode, SqlPlanNodeVisitor, SqlSelectColumn
from metricflow.sql.sql_select_node import SqlSelectStatementNode
from metricflow.sql.sql_table_node import SqlTableNode


@dataclass(frozen=True, eq=False)
class SqlCteNode(SqlPlanNode):
    """Represents a single common table expression."""

    select_statement: SqlPlanNode
    cte_alias: str

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1

    @staticmethod
    def create(select_statement: SqlPlanNode, cte_alias: str) -> SqlCteNode:  # noqa: D102
        return SqlCteNode(
            parent_nodes=(select_statement,),
            select_statement=select_statement,
            cte_alias=cte_alias,
        )

    def with_new_select(self, new_select_statement: SqlPlanNode) -> SqlCteNode:
        """Return a node with the same attributes but with the new SELECT statement."""
        return SqlCteNode.create(
            select_statement=new_select_statement,
            cte_alias=self.cte_alias,
        )

    @override
    def accept(self, visitor: SqlPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_cte_node(self)

    @property
    @override
    def as_select_node(self) -> Optional[SqlSelectStatementNode]:
        return None

    @property
    @override
    def as_sql_table_node(self) -> Optional[SqlTableNode]:
        return None

    @property
    @override
    def description(self) -> str:
        return "CTE"

    @classmethod
    @override
    def id_prefix(cls) -> IdPrefix:
        return StaticIdPrefix.SQL_PLAN_COMMON_TABLE_EXPRESSION_ID_PREFIX

    @override
    def nearest_select_columns(self, cte_source_mapping: SqlCteAliasMapping) -> Optional[Sequence[SqlSelectColumn]]:
        return self.select_statement.nearest_select_columns(cte_source_mapping)

    @override
    def copy(self) -> SqlCteNode:
        return SqlCteNode.create(
            select_statement=self.select_statement.copy(),
            cte_alias=self.cte_alias,
        )


@dataclass(frozen=True)
class SqlCteAliasMapping(Mergeable):
    """Thin, dict-like object that maps an alias to the associated `SqlCteNode`.

    When merged, the entries from the right mapping take precedence over the entries from the left.
    """

    cte_alias_to_cte_node_items: Tuple[Tuple[str, SqlCteNode], ...] = ()

    @staticmethod
    def create(cte_alias_to_cte_node_mapping: Mapping[str, SqlCteNode]) -> SqlCteAliasMapping:  # noqa: D102
        cte_alias_to_cte_node_pairs = []
        for cte_alias, cte_node in cte_alias_to_cte_node_mapping.items():
            cte_alias_to_cte_node_pairs.append((cte_alias, cte_node))

        return SqlCteAliasMapping(cte_alias_to_cte_node_items=tuple(cte_alias_to_cte_node_pairs))

    @cached_property
    def _cte_alias_to_cte_node_dict(self) -> Mapping[str, SqlCteNode]:
        return {item[0]: item[1] for item in self.cte_alias_to_cte_node_items}

    def get_cte_node_for_alias(self, cte_alias: str) -> Optional[SqlCteNode]:
        """Return the associated `SqlCteNode` for the given alias, or None if the given alias is not known."""
        return self._cte_alias_to_cte_node_dict.get(cte_alias)

    @override
    def merge(self, other: SqlCteAliasMapping) -> SqlCteAliasMapping:
        new_mapping = dict(self._cte_alias_to_cte_node_dict)
        for cte_alias, cte_node in other.cte_alias_to_cte_node_items:
            new_mapping[cte_alias] = cte_node
        return SqlCteAliasMapping.create(new_mapping)

    @classmethod
    @override
    def empty_instance(cls) -> SqlCteAliasMapping:
        return SqlCteAliasMapping()
