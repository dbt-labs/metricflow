from __future__ import annotations

import typing
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence, Tuple

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.sql.sql_exprs import SqlExpressionNode
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.toolkit.visitor import VisitorOutputT
from typing_extensions import override

from metricflow.sql.sql_plan import SqlPlanNode, SqlPlanNodeVisitor, SqlSelectColumn
from metricflow.sql.sql_table_node import SqlTableNode

if typing.TYPE_CHECKING:
    from metricflow.sql.sql_cte_node import SqlCteAliasMapping, SqlCteNode


@dataclass(frozen=True)
class SqlJoinDescription:
    """Describes how sources should be joined together."""

    # The source that goes on the right side of the JOIN keyword.
    right_source: SqlPlanNode
    right_source_alias: str
    join_type: SqlJoinType
    on_condition: Optional[SqlExpressionNode] = None

    def with_right_source(self, new_right_source: SqlPlanNode) -> SqlJoinDescription:
        """Return a copy of this but with the right source replaced."""
        return SqlJoinDescription(
            right_source=new_right_source,
            right_source_alias=self.right_source_alias,
            join_type=self.join_type,
            on_condition=self.on_condition,
        )


@dataclass(frozen=True)
class SqlOrderByDescription:  # noqa: D101
    expr: SqlExpressionNode
    desc: bool


@dataclass(frozen=True, eq=False)
class SqlSelectStatementNode(SqlPlanNode):
    """Represents an SQL Select statement.

    Attributes:
        select_columns: The columns to select.
        from_source: The source of the data for the select statement.
        from_source_alias: Alias for the from source.
        join_descs: Descriptions of the joins to perform.
        group_bys: The columns to group by.
        order_bys: The columns to order by.
        where: The where clause expression.
        limit: The limit of the number of rows to return.
        distinct: Whether the select statement should return distinct rows.
    """

    _description: str
    select_columns: Tuple[SqlSelectColumn, ...]
    from_source: SqlPlanNode
    from_source_alias: str
    cte_sources: Tuple[SqlCteNode, ...]
    join_descs: Tuple[SqlJoinDescription, ...]
    group_bys: Tuple[SqlSelectColumn, ...]
    order_bys: Tuple[SqlOrderByDescription, ...]
    where: Optional[SqlExpressionNode]
    limit: Optional[int]
    distinct: bool

    @staticmethod
    def create(  # noqa: D102
        description: str,
        select_columns: Iterable[SqlSelectColumn],
        from_source: SqlPlanNode,
        from_source_alias: str,
        cte_sources: Tuple[SqlCteNode, ...] = (),
        join_descs: Tuple[SqlJoinDescription, ...] = (),
        group_bys: Tuple[SqlSelectColumn, ...] = (),
        order_bys: Tuple[SqlOrderByDescription, ...] = (),
        where: Optional[SqlExpressionNode] = None,
        limit: Optional[int] = None,
        distinct: bool = False,
    ) -> SqlSelectStatementNode:
        parent_nodes = (from_source,) + tuple(x.right_source for x in join_descs) + cte_sources
        return SqlSelectStatementNode(
            parent_nodes=parent_nodes,
            _description=description,
            select_columns=tuple(select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
            cte_sources=cte_sources,
            join_descs=join_descs,
            group_bys=group_bys,
            order_bys=order_bys,
            where=where,
            limit=limit,
            distinct=distinct,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_PLAN_SELECT_STATEMENT_ID_PREFIX

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return (
            tuple(super().displayed_properties)
            + tuple(DisplayedProperty(f"col{i}", column) for i, column in enumerate(self.select_columns))
            + (DisplayedProperty("from_source", self.from_source),)
            + tuple(DisplayedProperty(f"join_{i}", join_desc) for i, join_desc in enumerate(self.join_descs))
            + tuple(DisplayedProperty(f"group_by{i}", group_by) for i, group_by in enumerate(self.group_bys))
            + (DisplayedProperty("where", self.where),)
            + tuple(DisplayedProperty(f"order_by{i}", order_by) for i, order_by in enumerate(self.order_bys))
            + (DisplayedProperty("distinct", self.distinct),)
        )

    def accept(self, visitor: SqlPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_select_statement_node(self)

    @property
    def as_select_node(self) -> Optional[SqlSelectStatementNode]:  # noqa: D102
        return self

    @property
    @override
    def as_sql_table_node(self) -> Optional[SqlTableNode]:
        return None

    @property
    @override
    def description(self) -> str:
        return self._description

    @override
    def nearest_select_columns(self, cte_source_mapping: SqlCteAliasMapping) -> Optional[Sequence[SqlSelectColumn]]:
        return self.select_columns

    @override
    def copy(self) -> SqlSelectStatementNode:
        return SqlSelectStatementNode.create(
            description=self._description,
            select_columns=self.select_columns,
            from_source=self.from_source.copy(),
            from_source_alias=self.from_source_alias,
            cte_sources=tuple(node.copy() for node in self.cte_sources),
            join_descs=tuple(
                join_desc.with_right_source(join_desc.right_source.copy()) for join_desc in self.join_descs
            ),
            group_bys=self.group_bys,
            order_bys=self.order_bys,
            where=self.where,
            limit=self.limit,
            distinct=self.distinct,
        )

    def with_select_columns(self, select_columns: Iterable[SqlSelectColumn]) -> SqlSelectStatementNode:
        """Return a copy with the select columns replaced."""
        return SqlSelectStatementNode.create(
            description=self.description,
            select_columns=tuple(select_columns),
            from_source=self.from_source,
            from_source_alias=self.from_source_alias,
            cte_sources=self.cte_sources,
            join_descs=self.join_descs,
            group_bys=self.group_bys,
            order_bys=self.order_bys,
            where=self.where,
            limit=self.limit,
            distinct=self.distinct,
        )

    def with_where_clause(self, where: Optional[SqlExpressionNode]) -> SqlSelectStatementNode:
        """Return a copy with the `WHERE` clause replaced."""
        return SqlSelectStatementNode.create(
            description=self.description,
            select_columns=self.select_columns,
            from_source=self.from_source,
            from_source_alias=self.from_source_alias,
            cte_sources=self.cte_sources,
            join_descs=self.join_descs,
            group_bys=self.group_bys,
            order_bys=self.order_bys,
            where=where,
            limit=self.limit,
            distinct=self.distinct,
        )
