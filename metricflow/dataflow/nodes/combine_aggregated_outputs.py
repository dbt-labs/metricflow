from __future__ import annotations

from typing import Sequence, Union

from metricflow.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow.dataflow.dataflow_plan import (
    BaseOutput,
    ComputedMetricsOutput,
    DataflowPlanNode,
    DataflowPlanNodeVisitor,
)
from metricflow.visitor import VisitorOutputT


class CombineAggregatedOutputsNode(ComputedMetricsOutput):
    """Combines metrics from different nodes into a single output."""

    def __init__(  # noqa: D
        self,
        parent_nodes: Sequence[Union[BaseOutput, ComputedMetricsOutput]],
    ) -> None:
        super().__init__(node_id=self.create_unique_id(), parent_nodes=parent_nodes)

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D
        return StaticIdPrefix.DATAFLOW_NODE_COMBINE_AGGREGATED_OUTPUTS_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_combine_aggregated_outputs_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return "Combine Aggregated Outputs"

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        return isinstance(other_node, self.__class__)

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> CombineAggregatedOutputsNode:  # noqa: D
        assert len(new_parent_nodes) == 1
        return CombineAggregatedOutputsNode(parent_nodes=new_parent_nodes)
