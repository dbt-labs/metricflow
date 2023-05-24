"""Nodes used in defining an SQL query plan."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Generic, List, Optional, Sequence, Tuple

from metricflow.dag.id_generation import (
    SQL_PLAN_SELECT_STATEMENT_ID_PREFIX,
    SQL_PLAN_TABLE_FROM_CLAUSE_ID_PREFIX,
)
from metricflow.dag.mf_dag import DagNode, DisplayedProperty, MetricFlowDag, NodeId
from metricflow.dataflow.sql_table import SqlTable
from metricflow.sql.sql_exprs import SqlExpressionNode
from metricflow.visitor import VisitorOutputT

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

    def __init__(self, node_id: NodeId, parent_nodes: Sequence[SqlQueryPlanNode]) -> None:  # noqa: D:
        self._parent_nodes = parent_nodes
        super().__init__(node_id=node_id)

    @property
    def parent_nodes(self) -> List[SqlQueryPlanNode]:  # noqa: D
        return list(self._parent_nodes)

    @abstractmethod
    def accept(self, visitor: SqlQueryPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        """Called when a visitor needs to visit this node."""
        pass

    @property
    @abstractmethod
    def is_table(self) -> bool:
        """Returns whether this node resolves to a table (vs. a query)."""
        pass

    @property
    @abstractmethod
    def as_select_node(self) -> Optional[SqlSelectStatementNode]:
        """If possible, return this as a select statement node."""
        pass


class SqlQueryPlanNodeVisitor(Generic[VisitorOutputT], ABC):
    """An object that can be used to visit the nodes of an SQL query.

    See similar visitor DataflowPlanVisitor.
    """

    @abstractmethod
    def visit_select_statement_node(self, node: SqlSelectStatementNode) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_table_from_clause_node(self, node: SqlTableFromClauseNode) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_query_from_clause_node(self, node: SqlSelectQueryFromClauseNode) -> VisitorOutputT:  # noqa: D
        pass


@dataclass(frozen=True)
class SqlSelectColumn:
    """Represents a column in the select clause of an SQL query."""

    expr: SqlExpressionNode
    # Always require a column alias for simplicity.
    column_alias: str


class SqlJoinType(Enum):
    """Enumerates the different kinds of SQL joins.

    The value is the SQL string to be used when rendering the join.
    """

    LEFT_OUTER = "LEFT OUTER JOIN"
    FULL_OUTER = "FULL OUTER JOIN"
    INNER = "INNER JOIN"
    CROSS_JOIN = "CROSS JOIN"

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}.{self.name}"


@dataclass(frozen=True)
class SqlJoinDescription:
    """Describes how sources should be joined together."""

    # The source that goes on the right side of the JOIN keyword.
    right_source: SqlQueryPlanNode
    right_source_alias: str
    join_type: SqlJoinType
    on_condition: Optional[SqlExpressionNode] = None


@dataclass(frozen=True)
class SqlOrderByDescription:  # noqa: D
    expr: SqlExpressionNode
    desc: bool


class SqlSelectStatementNode(SqlQueryPlanNode):
    """Represents an SQL Select statement."""

    def __init__(  # noqa: D
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

        if limit is not None:
            assert limit >= 0
        self._limit = limit

        super().__init__(
            node_id=self.create_unique_id(),
            parent_nodes=[self._from_source] + [x.right_source for x in self._join_descs],
        )

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_PLAN_SELECT_STATEMENT_ID_PREFIX

    @property
    def description(self) -> str:  # noqa: D
        return self._description

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return (
            super().displayed_properties
            + [DisplayedProperty(f"col{i}", column) for i, column in enumerate(self._select_columns)]
            + [DisplayedProperty("from_source", self.from_source)]
            + [DisplayedProperty(f"join_{i}", join_desc) for i, join_desc in enumerate(self._join_descs)]
            + [DisplayedProperty(f"group_by{i}", group_by) for i, group_by in enumerate(self._group_bys)]
            + [DisplayedProperty("where", self._where)]
            + [DisplayedProperty(f"order_by{i}", order_by) for i, order_by in enumerate(self._order_bys)]
        )

    @property
    def select_columns(self) -> Tuple[SqlSelectColumn, ...]:  # noqa: D
        return self._select_columns

    @property
    def from_source(self) -> SqlQueryPlanNode:  # noqa: D
        return self._from_source

    @property
    def from_source_alias(self) -> str:  # noqa: D
        return self._from_source_alias

    @property
    def join_descs(self) -> Tuple[SqlJoinDescription, ...]:  # noqa: D
        return self._join_descs

    @property
    def group_bys(self) -> Tuple[SqlSelectColumn, ...]:  # noqa: D
        return self._group_bys

    @property
    def where(self) -> Optional[SqlExpressionNode]:  # noqa: D
        return self._where

    @property
    def order_bys(self) -> Tuple[SqlOrderByDescription, ...]:  # noqa: D
        return self._order_bys

    def accept(self, visitor: SqlQueryPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_select_statement_node(self)

    @property
    def is_table(self) -> bool:  # noqa: D
        return False

    @property
    def limit(self) -> Optional[int]:  # noqa: D
        return self._limit

    @property
    def as_select_node(self) -> Optional[SqlSelectStatementNode]:  # noqa: D
        return self


class SqlTableFromClauseNode(SqlQueryPlanNode):
    """An SQL table that can go in the FROM clause."""

    def __init__(self, sql_table: SqlTable) -> None:  # noqa: D
        self._sql_table = sql_table
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_PLAN_TABLE_FROM_CLAUSE_ID_PREFIX

    @property
    def description(self) -> str:  # noqa: D
        return f"Read from {self._sql_table.sql}"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [
            DisplayedProperty("table_id", self._sql_table.sql),
        ]

    def accept(self, visitor: SqlQueryPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_table_from_clause_node(self)

    @property
    def sql_table(self) -> SqlTable:  # noqa: D
        return self._sql_table

    @property
    def is_table(self) -> bool:  # noqa: D
        return True

    @property
    def as_select_node(self) -> Optional[SqlSelectStatementNode]:  # noqa: D
        return None


class SqlSelectQueryFromClauseNode(SqlQueryPlanNode):
    """An SQL select query that can go in the FROM clause."""

    def __init__(self, select_query: str) -> None:  # noqa: D
        self._select_query = select_query
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_PLAN_TABLE_FROM_CLAUSE_ID_PREFIX

    @property
    def description(self) -> str:  # noqa: D
        return "Read From a Select Query"

    def accept(self, visitor: SqlQueryPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_query_from_clause_node(self)

    @property
    def select_query(self) -> str:  # noqa: D
        return self._select_query

    @property
    def is_table(self) -> bool:  # noqa: D
        return False

    @property
    def as_select_node(self) -> Optional[SqlSelectStatementNode]:  # noqa: D
        return None


class SqlQueryPlan(MetricFlowDag[SqlQueryPlanNode]):  # noqa: D
    """Model for an SQL Query as a DAG."""

    def __init__(self, plan_id: str, render_node: SqlQueryPlanNode) -> None:
        """Constructor.

        Args:
            plan_id: The ID to associate with this plan.
            render_node: The node from which to start rendering the SQL query.
        """
        self._render_node = render_node
        super().__init__(dag_id=plan_id, sink_nodes=[self._render_node])

    @property
    def render_node(self) -> SqlQueryPlanNode:  # noqa: D
        return self._render_node
