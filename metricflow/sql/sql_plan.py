"""Nodes used in defining an SQL query plan."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Mapping, Optional, Sequence, Tuple

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DagId, DagNode, DisplayedProperty, MetricFlowDag
from metricflow_semantics.sql.sql_exprs import SqlExpressionNode
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.visitor import VisitorOutputT
from typing_extensions import override

logger = logging.getLogger(__name__)


@dataclass(frozen=True, eq=False)
class SqlQueryPlanNode(DagNode["SqlQueryPlanNode"], ABC):
    """Modeling a SQL query plan like a data flow plan as well.

    In that model:
    * A source node that takes data into the graph e.g. SQL tables or SQL queries in a literal string format.
    * SELECT queries can be modeled as nodes that transform the data.
    * Statements like ALTER TABLE don't fit well, but they could be modeled as just a single sink node.
    * SQL queries in where conditions could be modeled as another SqlQueryPlan.
    * SqlRenderableNode() indicates nodes where plan generation can begin. Generally, this will be all nodes except
      the SqlTableNode() since my_table.my_column wouldn't be a valid SQL query.

    Is there an existing library that can do this?
    """

    @abstractmethod
    def accept(self, visitor: SqlQueryPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        """Called when a visitor needs to visit this node."""
        raise NotImplementedError

    @property
    @abstractmethod
    def as_select_node(self) -> Optional[SqlSelectStatementNode]:
        """If possible, return this as a select statement node."""
        raise NotImplementedError

    @property
    @abstractmethod
    def as_sql_table_node(self) -> Optional[SqlTableNode]:
        """If possible, return this as SQL table node."""
        raise NotImplementedError

    @abstractmethod
    def nearest_select_columns(
        self, cte_source_mapping: Mapping[str, SqlCteNode]
    ) -> Optional[Sequence[SqlSelectColumn]]:
        """Return the SELECT columns that are in this node or the closest `SqlSelectStatementNode` of its ancestors.

        * For a SELECT statement node, this is just the columns in the node.
        * For a node that has a SELECT statement node as its only parent (e.g. CREATE TABLE ... AS SELECT ...), this
          would be the SELECT columns in the parent.
        * If not known (e.g. an arbitrary SQL statement as a string), return None.
        * This is used to figure out which columns are needed at a leaf node of the DAG for column pruning.
        * A SQL table could refer to a CTE, so a mapping from the name of the CTE to the CTE node should be provided to
          get the associated SELECT columns.
        """
        raise NotImplementedError


class SqlQueryPlanNodeVisitor(Generic[VisitorOutputT], ABC):
    """An object that can be used to visit the nodes of an SQL query.

    See similar visitor DataflowPlanVisitor.
    """

    @abstractmethod
    def visit_select_statement_node(self, node: SqlSelectStatementNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_table_node(self, node: SqlTableNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_query_from_clause_node(self, node: SqlSelectQueryFromClauseNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_create_table_as_node(self, node: SqlCreateTableAsNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_cte_node(self, node: SqlCteNode) -> VisitorOutputT:  # noqa: D102
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

    def with_right_source(self, new_right_source: SqlQueryPlanNode) -> SqlJoinDescription:
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
class SqlSelectStatementNode(SqlQueryPlanNode):
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
    from_source: SqlQueryPlanNode
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
        select_columns: Tuple[SqlSelectColumn, ...],
        from_source: SqlQueryPlanNode,
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
            select_columns=select_columns,
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

    def accept(self, visitor: SqlQueryPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
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
    def nearest_select_columns(
        self, cte_source_mapping: Mapping[str, SqlCteNode]
    ) -> Optional[Sequence[SqlSelectColumn]]:
        return self.select_columns

    def create_copy(self) -> SqlSelectStatementNode:  # noqa: D102
        return SqlSelectStatementNode.create(
            description=self.description,
            select_columns=self.select_columns,
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


@dataclass(frozen=True, eq=False)
class SqlTableNode(SqlQueryPlanNode):
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

    def accept(self, visitor: SqlQueryPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_table_node(self)

    @property
    def as_select_node(self) -> Optional[SqlSelectStatementNode]:  # noqa: D102
        return None

    @override
    def nearest_select_columns(
        self, cte_source_mapping: Mapping[str, SqlCteNode]
    ) -> Optional[Sequence[SqlSelectColumn]]:
        if self.sql_table.schema_name is None:
            cte_node = cte_source_mapping.get(self.sql_table.table_name)
            if cte_node is not None:
                return cte_node.nearest_select_columns(cte_source_mapping)
        return None

    @property
    @override
    def as_sql_table_node(self) -> Optional[SqlTableNode]:
        return self


@dataclass(frozen=True, eq=False)
class SqlSelectQueryFromClauseNode(SqlQueryPlanNode):
    """An SQL select query that can go in the FROM clause.

    Attributes:
        select_query: The SQL select query to include in the FROM clause.
    """

    select_query: str

    @staticmethod
    def create(select_query: str) -> SqlSelectQueryFromClauseNode:  # noqa: D102
        return SqlSelectQueryFromClauseNode(
            parent_nodes=(),
            select_query=select_query,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.SQL_PLAN_QUERY_FROM_CLAUSE_ID_PREFIX

    @property
    def description(self) -> str:  # noqa: D102
        return "Read From a Select Query"

    def accept(self, visitor: SqlQueryPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_query_from_clause_node(self)

    @property
    def as_select_node(self) -> Optional[SqlSelectStatementNode]:  # noqa: D102
        return None

    @override
    def nearest_select_columns(
        self, cte_source_mapping: Mapping[str, SqlCteNode]
    ) -> Optional[Sequence[SqlSelectColumn]]:
        return None

    @property
    @override
    def as_sql_table_node(self) -> Optional[SqlTableNode]:
        return None


@dataclass(frozen=True, eq=False)
class SqlCreateTableAsNode(SqlQueryPlanNode):
    """An SQL node representing a CREATE TABLE AS statement.

    Attributes:
        sql_table: The SQL table to create.
    """

    sql_table: SqlTable

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1

    @staticmethod
    def create(sql_table: SqlTable, parent_node: SqlQueryPlanNode) -> SqlCreateTableAsNode:  # noqa: D102
        return SqlCreateTableAsNode(
            parent_nodes=(parent_node,),
            sql_table=sql_table,
        )

    @override
    def accept(self, visitor: SqlQueryPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
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
    def parent_node(self) -> SqlQueryPlanNode:  # noqa: D102
        return self.parent_nodes[0]

    @classmethod
    @override
    def id_prefix(cls) -> IdPrefix:
        return StaticIdPrefix.SQL_PLAN_CREATE_TABLE_AS_ID_PREFIX

    @override
    def nearest_select_columns(
        self, cte_source_mapping: Mapping[str, SqlCteNode]
    ) -> Optional[Sequence[SqlSelectColumn]]:
        return self.parent_node.nearest_select_columns(cte_source_mapping)


class SqlPlan(MetricFlowDag[SqlQueryPlanNode]):
    """Model for an SQL statement as a DAG."""

    def __init__(self, render_node: SqlQueryPlanNode, plan_id: Optional[DagId] = None) -> None:
        """initializer.

        Args:
            render_node: The node from which to start rendering the SQL statement.
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


@dataclass(frozen=True, eq=False)
class SqlCteNode(SqlQueryPlanNode):
    """Represents a single common table expression."""

    select_statement: SqlQueryPlanNode
    cte_alias: str

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1

    @staticmethod
    def create(select_statement: SqlQueryPlanNode, cte_alias: str) -> SqlCteNode:  # noqa: D102
        return SqlCteNode(
            parent_nodes=(select_statement,),
            select_statement=select_statement,
            cte_alias=cte_alias,
        )

    def with_new_select(self, new_select_statement: SqlQueryPlanNode) -> SqlCteNode:
        """Return a node with the same attributes but with the new SELECT statement."""
        return SqlCteNode.create(
            select_statement=new_select_statement,
            cte_alias=self.cte_alias,
        )

    @override
    def accept(self, visitor: SqlQueryPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
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
    def nearest_select_columns(
        self, cte_source_mapping: Mapping[str, SqlCteNode]
    ) -> Optional[Sequence[SqlSelectColumn]]:
        return self.select_statement.nearest_select_columns(cte_source_mapping)
