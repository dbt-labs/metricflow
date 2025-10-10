from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.toolkit.visitor import VisitorOutputT
from typing_extensions import override

from metricflow.sql.sql_cte_node import SqlCteAliasMapping
from metricflow.sql.sql_plan import SqlPlanNode, SqlPlanNodeVisitor, SqlSelectColumn
from metricflow.sql.sql_select_node import SqlSelectStatementNode
from metricflow.sql.sql_table_node import SqlTableNode


@dataclass(frozen=True, eq=False)
class SqlSelectTextNode(SqlPlanNode):
    """An SQL select query that can go in the FROM clause.

    Attributes:
        select_query: The SQL select query to include in the FROM clause.
    """

    select_query: str

    @staticmethod
    def create(select_query: str) -> SqlSelectTextNode:  # noqa: D102
        return SqlSelectTextNode(
            parent_nodes=(),
            select_query=select_query,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_PLAN_QUERY_FROM_CLAUSE_ID_PREFIX

    @property
    def description(self) -> str:  # noqa: D102
        return "Read From a Select Query"

    def accept(self, visitor: SqlPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_query_from_clause_node(self)

    @property
    def as_select_node(self) -> Optional[SqlSelectStatementNode]:  # noqa: D102
        return None

    @override
    def nearest_select_columns(self, cte_source_mapping: SqlCteAliasMapping) -> Optional[Sequence[SqlSelectColumn]]:
        return None

    @property
    @override
    def as_sql_table_node(self) -> Optional[SqlTableNode]:
        return None

    @override
    def copy(self) -> SqlSelectTextNode:
        return SqlSelectTextNode(
            parent_nodes=tuple(node.copy() for node in self.parent_nodes), select_query=self.select_query
        )
