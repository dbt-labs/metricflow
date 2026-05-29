"""Nodes used in defining an SQL query plan."""

from __future__ import annotations

import logging
import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Optional, Sequence

from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DagId, DagNode, MetricFlowDag
from metricflow_semantics.sql.sql_exprs import SqlColumnReferenceExpression, SqlExpressionNode
from metricflow_semantics.toolkit.visitor import VisitorOutputT
from typing_extensions import Self

if typing.TYPE_CHECKING:
    from metricflow.sql.sql_ctas_node import SqlCreateTableAsNode
    from metricflow.sql.sql_cte_node import SqlCteAliasMapping, SqlCteNode
    from metricflow.sql.sql_select_node import SqlSelectStatementNode
    from metricflow.sql.sql_select_text_node import SqlSelectTextNode
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
        """Return a copy of the branch represented by this node.

        The node fields are copied by reference, similar to shallow copying.
        """
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
    def visit_query_from_clause_node(self, node: SqlSelectTextNode) -> VisitorOutputT:  # noqa: D102
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

    def copy_with_new_alias(self, column_alias: str) -> SqlSelectColumn:
        """Return a copy with the `column_alias` replaced with the given value."""
        return SqlSelectColumn(expr=self.expr, column_alias=column_alias)


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
