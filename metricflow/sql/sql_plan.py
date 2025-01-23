"""Nodes used in defining an SQL query plan."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from typing import Generic, Mapping, Optional, Sequence, Tuple

from metricflow_semantics.collection_helpers.merger import Mergeable
from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DagId, DagNode, MetricFlowDag
from metricflow_semantics.sql.sql_exprs import SqlColumnReferenceExpression, SqlExpressionNode
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.visitor import VisitorOutputT
from typing_extensions import Self, override

from metricflow.sql.sql_select_node import SqlSelectStatementNode
from metricflow.sql.sql_table_node import SqlTableNode

logger = logging.getLogger(__name__)


@dataclass(frozen=True, eq=False)
class SqlPlanNode(DagNode["SqlPlanNode"], ABC):
    """A node in the SQL plan model.

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
    def accept(self, visitor: SqlPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
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
    def nearest_select_columns(self, cte_source_mapping: SqlCteAliasMapping) -> Optional[Sequence[SqlSelectColumn]]:
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

    @abstractmethod
    def copy(self) -> Self:
        """Create a shallow copy of this node."""
        raise NotImplementedError


class SqlPlanNodeVisitor(Generic[VisitorOutputT], ABC):
    """An object that can be used to visit the nodes of an SQL plan.

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

    @staticmethod
    def from_column_reference(table_alias: str, column_name: str) -> SqlSelectColumn:
        """Create a column that selects a column from a table by name."""
        return SqlSelectColumn(
            expr=SqlColumnReferenceExpression.from_column_reference(column_name=column_name, table_alias=table_alias),
            column_alias=column_name,
        )

    def reference_from(self, source_table_alias: str) -> SqlColumnReferenceExpression:
        """Return a column reference expression for this column with a new table alias.

        Useful when you already have access to the select column from a subquery and want to reference it in an outer query.
        """
        return SqlColumnReferenceExpression.from_column_reference(
            column_name=self.column_alias, table_alias=source_table_alias
        )


@dataclass(frozen=True, eq=False)
class SqlSelectQueryFromClauseNode(SqlPlanNode):
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
    def copy(self) -> SqlSelectQueryFromClauseNode:
        return SqlSelectQueryFromClauseNode(parent_nodes=self.parent_nodes, select_query=self.select_query)


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
        return SqlCreateTableAsNode(parent_nodes=self.parent_nodes, sql_table=self.sql_table)


class SqlPlan(MetricFlowDag[SqlPlanNode]):
    """Model for an SQL statement as a DAG."""

    def __init__(self, render_node: SqlPlanNode, plan_id: Optional[DagId] = None) -> None:
        """initializer.

        Args:
            render_node: The node from which to start rendering the SQL statement.
            plan_id: If specified, use this sql_query_plan_id instead of a generated one.
        """
        self._render_node = render_node
        super().__init__(
            dag_id=plan_id or DagId.from_id_prefix(StaticIdPrefix.SQL_PLAN_PREFIX),
            sink_nodes=[self._render_node],
        )

    @property
    def render_node(self) -> SqlPlanNode:  # noqa: D102
        return self._render_node


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
        return SqlCteNode(
            parent_nodes=self.parent_nodes,
            select_statement=self.select_statement,
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
