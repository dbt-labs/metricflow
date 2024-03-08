from __future__ import annotations

from typing import Sequence

from metricflow.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow.dataflow.dataflow_plan import BaseOutput, DataflowPlanNode, DataflowPlanNodeVisitor
from metricflow.visitor import VisitorOutputT


class MinMaxNode(BaseOutput):
    """Calculate the min and max of a single instance data set."""

    def __init__(self, parent_node: BaseOutput) -> None:  # noqa: D
        self._parent_node = parent_node
        super().__init__(node_id=self.create_unique_id(), parent_nodes=(parent_node,))

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D
        return StaticIdPrefix.DATAFLOW_NODE_MIN_MAX_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_min_max_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return "Calculate min and max"

    @property
    def parent_node(self) -> BaseOutput:  # noqa: D
        return self._parent_node

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        return isinstance(other_node, self.__class__)

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> MinMaxNode:  # noqa: D
        assert len(new_parent_nodes) == 1
        return MinMaxNode(parent_node=new_parent_nodes[0])
