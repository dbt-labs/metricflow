from __future__ import annotations

from typing import Sequence, Union

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
    DataflowPlanNodeVisitor,
)


class CombineAggregatedOutputsNode(DataflowPlanNode):
    """Combines metrics from different nodes into a single output."""

    def __init__(  # noqa: D107
        self,
        parent_nodes: Sequence[Union[DataflowPlanNode, DataflowPlanNode]],
    ) -> None:
        super().__init__(node_id=self.create_unique_id(), parent_nodes=parent_nodes)

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_COMBINE_AGGREGATED_OUTPUTS_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_combine_aggregated_outputs_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return "Combine Aggregated Outputs"

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return isinstance(other_node, self.__class__)

    def with_new_parents(  # noqa: D102
        self, new_parent_nodes: Sequence[DataflowPlanNode]
    ) -> CombineAggregatedOutputsNode:
        assert len(new_parent_nodes) == 1
        return CombineAggregatedOutputsNode(parent_nodes=new_parent_nodes)
