from __future__ import annotations

from typing import Sequence

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
    DataflowPlanNodeVisitor,
)


class WriteToResultDataTableNode(DataflowPlanNode):
    """A node where incoming data gets written to a data_table."""

    def __init__(self, parent_node: DataflowPlanNode) -> None:  # noqa: D107
        self._parent_node = parent_node
        super().__init__(node_id=self.create_unique_id(), parent_nodes=(parent_node,))

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_WRITE_TO_RESULT_DATA_TABLE_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_write_to_result_data_table_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return """Write to DataTable"""

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        assert len(self.parent_nodes) == 1
        return self._parent_node

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return isinstance(other_node, self.__class__)

    def with_new_parents(  # noqa: D102
        self, new_parent_nodes: Sequence[DataflowPlanNode]
    ) -> WriteToResultDataTableNode:
        assert len(new_parent_nodes) == 1
        return WriteToResultDataTableNode(parent_node=new_parent_nodes[0])
