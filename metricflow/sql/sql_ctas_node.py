from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.toolkit.visitor import VisitorOutputT
from typing_extensions import override

from metricflow.sql.sql_cte_node import SqlCteAliasMapping
from metricflow.sql.sql_plan import SqlPlanNode, SqlPlanNodeVisitor, SqlSelectColumn
from metricflow.sql.sql_select_node import SqlSelectStatementNode
from metricflow.sql.sql_table_node import SqlTableNode


@dataclass(frozen=True, eq=False)
class SqlCreateTableAsNode(SqlPlanNode):
    """An SQL node representing a CREATE TABLE AS statement.

    Attributes:
        sql_table: The SQL table to create.
    """

    sql_table: SqlTable

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1

    @staticmethod
    def create(sql_table: SqlTable, parent_node: SqlPlanNode) -> SqlCreateTableAsNode:  # noqa: D102
        return SqlCreateTableAsNode(
            parent_nodes=(parent_node,),
            sql_table=sql_table,
        )

    @override
    def accept(self, visitor: SqlPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_create_table_as_node(self)

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
        return f"Create table {repr(self.sql_table.sql)}"

    @property
    def parent_node(self) -> SqlPlanNode:  # noqa: D102
        return self.parent_nodes[0]

    @classmethod
    @override
    def id_prefix(cls) -> IdPrefix:
        return StaticIdPrefix.SQL_PLAN_CREATE_TABLE_AS_ID_PREFIX

    @override
    def nearest_select_columns(self, cte_source_mapping: SqlCteAliasMapping) -> Optional[Sequence[SqlSelectColumn]]:
        return self.parent_node.nearest_select_columns(cte_source_mapping)

    @override
    def copy(self) -> SqlCreateTableAsNode:
        return SqlCreateTableAsNode.create(parent_node=self.parent_node.copy(), sql_table=self.sql_table)
