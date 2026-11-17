from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.toolkit.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass(frozen=True, eq=False)
class JoinToBaseAggregatedOutputsNode(DataflowPlanNode):
    """Left joins aggregated outputs at a coarser grain onto a base aggregated output.

    The first parent is the base and has every linkable spec of the output. The remaining parents are declared
    inputs that are aggregated by a subset of the base's linkable specs. The output has the grain of the base.
    """

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        num_parents = len(self.parent_nodes)
        assert num_parents > 1, (
            "The JoinToBaseAggregatedOutputsNode requires a base node and at least one declared input, but this "
            f"node is being initialized with {num_parents} parent(s)."
        )

    @staticmethod
    def create(  # noqa: D102
        base_node: DataflowPlanNode, declared_input_nodes: Sequence[DataflowPlanNode]
    ) -> JoinToBaseAggregatedOutputsNode:
        return JoinToBaseAggregatedOutputsNode(parent_nodes=(base_node,) + tuple(declared_input_nodes))

    @property
    def base_node(self) -> DataflowPlanNode:  # noqa: D102
        return self.parent_nodes[0]

    @property
    def declared_input_nodes(self) -> Sequence[DataflowPlanNode]:  # noqa: D102
        return self.parent_nodes[1:]

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_JOIN_TO_BASE_AGGREGATED_OUTPUTS_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_join_to_base_aggregated_outputs_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return "Join Aggregated Outputs to Base"

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return isinstance(other_node, self.__class__) and len(self.declared_input_nodes) == len(
            other_node.declared_input_nodes
        )

    def with_new_parents(  # noqa: D102
        self, new_parent_nodes: Sequence[DataflowPlanNode]
    ) -> JoinToBaseAggregatedOutputsNode:
        return JoinToBaseAggregatedOutputsNode(parent_nodes=tuple(new_parent_nodes))
