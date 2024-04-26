"""Nodes used in defining an SQL query plan."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, List, Optional, Sequence, Tuple

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DagId, DagNode, DisplayedProperty, MetricFlowDag, NodeId
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.visitor import VisitorOutputT
from typing_extensions import override

from metricflow.sql.sql_exprs import SqlExpressionNode
from metricflow.sql.sql_table import SqlTable

logger = logging.getLogger(__name__)


class SqlQueryPlanNode(DagNode, ABC):
    """Modeling a SQL query plan like a data flow plan as well.

    In that model:
    * A source node that takes data into the graph e.g. SQL tables or SQL queries in a literal string format.
    * SELECT queries can be modeled as nodes that transform the data.
    * Statements like ALTER TABLE don't fit well, but they could be modeled as just a single sink node.
    * SQL queries in where conditions could be modeled as another SqlQueryPlan.
    * SqlRenderableNode() indicates nodes where plan generation can begin. Generally, this will be all nodes except
      the SqlTableFromClauseNode() since my_table.my_column wouldn't be a valid SQL query.

    Is there an existing library that can do this?
    """

    def __init__(self, node_id: NodeId, parent_nodes: Sequence[SqlQueryPlanNode]) -> None:  # noqa: D107
        self._parent_nodes = parent_nodes
        super().__init__(node_id=node_id)

    @property
    def parent_nodes(self) -> List[SqlQueryPlanNode]:  # noqa: D102
        return list(self._parent_nodes)

    @abstractmethod
    def accept(self, visitor: SqlQueryPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        """Called when a visitor needs to visit this node."""
        raise NotImplementedError

    @property
    @abstractmethod
    def is_table(self) -> bool:
        """Returns whether this node resolves to a table (vs. a query)."""
        raise NotImplementedError

    @property
    @abstractmethod
    def as_select_node(self) -> Optional[SqlSelectStatementNode]:
        """If possible, return this as a select statement node."""
        raise NotImplementedError


class SqlQueryPlanNodeVisitor(Generic[VisitorOutputT], ABC):
    """An object that can be used to visit the nodes of an SQL query.

    See similar visitor DataflowPlanVisitor.
    """

    @abstractmethod
    def visit_select_statement_node(self, node: SqlSelectStatementNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_table_from_clause_node(self, node: SqlTableFromClauseNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_query_from_clause_node(self, node: SqlSelectQueryFromClauseNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_create_table_as_node(self, node: SqlCreateTableAsNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError


@dataclass(frozen=True)
class SqlSelectColumn:
    """Represents a column in the select clause of an SQL query."""

    expr: SqlExpressionNode
    # Always require a column alias for simplicity.
    column_alias: str


@dataclass(frozen=True)
class SqlJoinDescription:
    """Describes how sources should be joined together."""

    # The source that goes on the right side of the JOIN keyword.
    right_source: SqlQueryPlanNode
    right_source_alias: str
    join_type: SqlJoinType
    on_condition: Optional[SqlExpressionNode] = None


@dataclass(frozen=True)
class SqlOrderByDescription:  # noqa: D101
    expr: SqlExpressionNode
    desc: bool


class SqlSelectStatementNode(SqlQueryPlanNode):
    """Represents an SQL Select statement."""

    def __init__(  # noqa: D107
        self,
        description: str,
        select_columns: Tuple[SqlSelectColumn, ...],
        from_source: SqlQueryPlanNode,
        from_source_alias: str,
        joins_descs: Tuple[SqlJoinDescription, ...],
        group_bys: Tuple[SqlSelectColumn, ...],
        order_bys: Tuple[SqlOrderByDescription, ...],
        where: Optional[SqlExpressionNode] = None,
        limit: Optional[int] = None,
        distinct: bool = False,
    ) -> None:
        self._description = description
        assert select_columns
        self._select_columns = select_columns
        # Sources that belong in a from clause. CTEs could be captured in a separate field.
        self._from_source = from_source
        self._from_source_alias = from_source_alias
        self._join_descs = joins_descs
        self._group_bys = group_bys
        self._where = where
        self._order_bys = order_bys
        self._distinct = distinct

        if limit is not None:
            assert limit >= 0
        self._limit = limit

        super().__init__(
            node_id=self.create_unique_id(),
            parent_nodes=[self._from_source] + [x.right_source for x in self._join_descs],
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_PLAN_SELECT_STATEMENT_ID_PREFIX

    @property
    def description(self) -> str:  # noqa: D102
        return self._description

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return (
            tuple(super().displayed_properties)
            + tuple(DisplayedProperty(f"col{i}", column) for i, column in enumerate(self._select_columns))
            + (DisplayedProperty("from_source", self.from_source),)
            + tuple(DisplayedProperty(f"join_{i}", join_desc) for i, join_desc in enumerate(self._join_descs))
            + tuple(DisplayedProperty(f"group_by{i}", group_by) for i, group_by in enumerate(self._group_bys))
            + (DisplayedProperty("where", self._where),)
            + tuple(DisplayedProperty(f"order_by{i}", order_by) for i, order_by in enumerate(self._order_bys))
            + (DisplayedProperty("distinct", self._distinct),)
        )

    @property
    def select_columns(self) -> Tuple[SqlSelectColumn, ...]:  # noqa: D102
        return self._select_columns

    @property
    def from_source(self) -> SqlQueryPlanNode:  # noqa: D102
        return self._from_source

    @property
    def from_source_alias(self) -> str:  # noqa: D102
        return self._from_source_alias

    @property
    def join_descs(self) -> Tuple[SqlJoinDescription, ...]:  # noqa: D102
        return self._join_descs

    @property
    def group_bys(self) -> Tuple[SqlSelectColumn, ...]:  # noqa: D102
        return self._group_bys

    @property
    def where(self) -> Optional[SqlExpressionNode]:  # noqa: D102
        return self._where

    @property
    def order_bys(self) -> Tuple[SqlOrderByDescription, ...]:  # noqa: D102
        return self._order_bys

    def accept(self, visitor: SqlQueryPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_select_statement_node(self)

    @property
    def is_table(self) -> bool:  # noqa: D102
        return False

    @property
    def limit(self) -> Optional[int]:  # noqa: D102
        return self._limit

    @property
    def as_select_node(self) -> Optional[SqlSelectStatementNode]:  # noqa: D102
        return self

    @property
    def distinct(self) -> bool:  # noqa: D102
        return self._distinct


class SqlTableFromClauseNode(SqlQueryPlanNode):
    """An SQL table that can go in the FROM clause."""

    def __init__(self, sql_table: SqlTable) -> None:  # noqa: D107
        self._sql_table = sql_table
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[])

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_PLAN_TABLE_FROM_CLAUSE_ID_PREFIX

    @property
    def description(self) -> str:  # noqa: D102
        return f"Read from {self._sql_table.sql}"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (DisplayedProperty("table_id", self._sql_table.sql),)

    def accept(self, visitor: SqlQueryPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_table_from_clause_node(self)

    @property
    def sql_table(self) -> SqlTable:  # noqa: D102
        return self._sql_table

    @property
    def is_table(self) -> bool:  # noqa: D102
        return True

    @property
    def as_select_node(self) -> Optional[SqlSelectStatementNode]:  # noqa: D102
        return None


class SqlSelectQueryFromClauseNode(SqlQueryPlanNode):
    """An SQL select query that can go in the FROM clause."""

    def __init__(self, select_query: str) -> None:  # noqa: D107
        self._select_query = select_query
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[])

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_PLAN_QUERY_FROM_CLAUSE_ID_PREFIX

    @property
    def description(self) -> str:  # noqa: D102
        return "Read From a Select Query"

    def accept(self, visitor: SqlQueryPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_query_from_clause_node(self)

    @property
    def select_query(self) -> str:  # noqa: D102
        return self._select_query

    @property
    def is_table(self) -> bool:  # noqa: D102
        return False

    @property
    def as_select_node(self) -> Optional[SqlSelectStatementNode]:  # noqa: D102
        return None


class SqlCreateTableAsNode(SqlQueryPlanNode):
    """An SQL select query that can go in the FROM clause."""

    def __init__(self, sql_table: SqlTable, parent_node: SqlQueryPlanNode) -> None:  # noqa: D107
        self._sql_table = sql_table
        self._parent_node = parent_node
        super().__init__(node_id=self.create_unique_id(), parent_nodes=(self._parent_node,))

    @override
    def accept(self, visitor: SqlQueryPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_create_table_as_node(self)

    @property
    @override
    def is_table(self) -> bool:
        return False

    @property
    @override
    def as_select_node(self) -> Optional[SqlSelectStatementNode]:
        return None

    @property
    @override
    def description(self) -> str:
        return f"Create table {repr(self.sql_table.sql)}"

    @classmethod
    @override
    def id_prefix(cls) -> IdPrefix:
        return StaticIdPrefix.SQL_PLAN_CREATE_TABLE_AS_ID_PREFIX

    @property
    def sql_table(self) -> SqlTable:
        """Return the table that this statement would create."""
        return self._sql_table

    @property
    def parent_node(self) -> SqlQueryPlanNode:  # noqa: D102
        return self._parent_node


class SqlQueryPlan(MetricFlowDag[SqlQueryPlanNode]):
    """Model for an SQL Query as a DAG."""

    def __init__(self, render_node: SqlQueryPlanNode, plan_id: Optional[DagId] = None) -> None:
        """Constructor.

        Args:
            render_node: The node from which to start rendering the SQL query.
            plan_id: If specified, use this sql_query_plan_id instead of a generated one.
        """
        self._render_node = render_node
        super().__init__(
            dag_id=plan_id or DagId.from_id_prefix(StaticIdPrefix.SQL_QUERY_PLAN_PREFIX),
            sink_nodes=[self._render_node],
        )

    @property
    def render_node(self) -> SqlQueryPlanNode:  # noqa: D102
        return self._render_node
