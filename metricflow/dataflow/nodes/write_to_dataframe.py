from __future__ import annotations

from typing import Sequence

from metricflow.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow.dataflow.dataflow_plan import (
    BaseOutput,
    DataflowPlanNode,
    DataflowPlanNodeVisitor,
    SinkNodeVisitor,
    SinkOutput,
)
from metricflow.visitor import VisitorOutputT


class WriteToResultDataframeNode(SinkOutput):
    """A node where incoming data gets written to a dataframe."""

    def __init__(self, parent_node: BaseOutput) -> None:  # noqa: D107
        self._parent_node = parent_node
        super().__init__(node_id=self.create_unique_id(), parent_nodes=(parent_node,))

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_WRITE_TO_RESULT_DATAFRAME_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_write_to_result_dataframe_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return """Write to Dataframe"""

    @property
    def parent_node(self) -> BaseOutput:  # noqa: D102
        assert len(self.parent_nodes) == 1
        return self._parent_node

    def accept_sink_node_visitor(self, visitor: SinkNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_write_to_result_dataframe_node(self)

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return isinstance(other_node, self.__class__)

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> WriteToResultDataframeNode:  # noqa: D102
        assert len(new_parent_nodes) == 1
        return WriteToResultDataframeNode(parent_node=new_parent_nodes[0])
