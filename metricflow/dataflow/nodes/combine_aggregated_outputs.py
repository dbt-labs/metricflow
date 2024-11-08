from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
)
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass(frozen=True, eq=False)
class CombineAggregatedOutputsNode(DataflowPlanNode):
    """Combines metrics from different nodes into a single output."""

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        num_parents = len(self.parent_nodes)
        assert num_parents > 1, (
            "The CombineAggregatedOutputsNode is intended to merge the output datasets from 2 or more nodes, but this "
            f"node is being initialized with with only {num_parents} parent(s)."
        )

    @staticmethod
    def create(parent_nodes: Sequence[DataflowPlanNode]) -> CombineAggregatedOutputsNode:  # noqa: D102
        return CombineAggregatedOutputsNode(parent_nodes=tuple(parent_nodes))

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
        return CombineAggregatedOutputsNode(parent_nodes=tuple(new_parent_nodes))
