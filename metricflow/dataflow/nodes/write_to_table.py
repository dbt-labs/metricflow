from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.toolkit.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
)
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass(frozen=True, eq=False)
class WriteToResultTableNode(DataflowPlanNode):
    """A node where incoming data gets written to a table.

    Attributes:
        output_sql_table: The table where the computed metrics should be written to.
    """

    output_sql_table: SqlTable

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1

    @staticmethod
    def create(  # noqa: D102
        parent_node: DataflowPlanNode,
        output_sql_table: SqlTable,
    ) -> WriteToResultTableNode:
        return WriteToResultTableNode(
            parent_nodes=(parent_node,),
            output_sql_table=output_sql_table,
        )

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
        return self.parent_nodes[0]

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return isinstance(other_node, self.__class__) and other_node.output_sql_table == self.output_sql_table

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> WriteToResultTableNode:  # noqa: D102
        return WriteToResultTableNode.create(
            parent_node=new_parent_nodes[0],
            output_sql_table=self.output_sql_table,
        )
