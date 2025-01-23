from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, Optional, override

from metricflow.sql.sql_plan import SqlPlanNode, SqlPlanNodeVisitor, SqlCteAliasMapping, SqlSelectColumn
from metricflow.sql.sql_select_node import SqlSelectStatementNode
from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.visitor import VisitorOutputT


@dataclass(frozen=True, eq=False)
class SqlTableNode(SqlPlanNode):
    """An SQL table that can go in the FROM clause or the JOIN clause."""

    sql_table: SqlTable

    @staticmethod
    def create(sql_table: SqlTable) -> SqlTableNode:  # noqa: D102
        return SqlTableNode(
            parent_nodes=(),
            sql_table=sql_table,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_PLAN_TABLE_FROM_CLAUSE_ID_PREFIX

    @property
    def description(self) -> str:  # noqa: D102
        return f"Read from {self.sql_table.sql}"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (DisplayedProperty("table_id", self.sql_table.sql),)

    def accept(self, visitor: SqlPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_table_node(self)

    @property
    def as_select_node(self) -> Optional[SqlSelectStatementNode]:  # noqa: D102
        return None

    @override
    def nearest_select_columns(self, cte_source_mapping: SqlCteAliasMapping) -> Optional[Sequence[SqlSelectColumn]]:
        if self.sql_table.schema_name is None:
            cte_node = cte_source_mapping.get_cte_node_for_alias(self.sql_table.table_name)
            if cte_node is not None:
                return cte_node.nearest_select_columns(cte_source_mapping)
        return None

    @property
    @override
    def as_sql_table_node(self) -> Optional[SqlTableNode]:
        return self

    @override
    def copy(self) -> SqlTableNode:
        return SqlTableNode(
            parent_nodes=self.parent_nodes,
            sql_table=self.sql_table,
        )
