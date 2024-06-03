from __future__ import annotations

from typing import Sequence

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
    DataflowPlanNodeVisitor,
)
from metricflow.sql.sql_table import SqlTable


class WriteToResultTableNode(DataflowPlanNode):
    """A node where incoming data gets written to a table."""

    def __init__(
        self,
        parent_node: DataflowPlanNode,
        output_sql_table: SqlTable,
    ) -> None:
        """Constructor.

        Args:
            parent_node: node that outputs the computed metrics.
            output_sql_table: the table where the computed metrics should be written to.
        """
        self._parent_node = parent_node
        self._output_sql_table = output_sql_table
        super().__init__(node_id=self.create_unique_id(), parent_nodes=(parent_node,))

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_WRITE_TO_RESULT_DATA_TABLE_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_write_to_result_table_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return """Write to Table"""

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        assert len(self.parent_nodes) == 1
        return self._parent_node

    @property
    def output_sql_table(self) -> SqlTable:  # noqa: D102
        return self._output_sql_table

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return isinstance(other_node, self.__class__) and other_node.output_sql_table == self.output_sql_table

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> WriteToResultTableNode:  # noqa: D102
        return WriteToResultTableNode(
            parent_node=new_parent_nodes[0],
            output_sql_table=self.output_sql_table,
        )
