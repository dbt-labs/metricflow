from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Sequence

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass(frozen=True, eq=False)
class JoinToCustomGranularityNode(DataflowPlanNode, ABC):
    """Join parent dataset to time spine dataset to convert time dimension to a custom granularity.

    Args:
        time_dimension_spec: The time dimension spec with a custom granularity that will be satisfied by this node.
    """

    time_dimension_spec: TimeDimensionSpec

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert (
            self.time_dimension_spec.time_granularity.is_custom_granularity
        ), "Time granularity for time dimension spec in JoinToCustomGranularityNode must be qualified as custom granularity."
        f" Instead, found {self.time_dimension_spec.time_granularity.name}. This indicates internal misconfiguration."

    @staticmethod
    def create(  # noqa: D102
        parent_node: DataflowPlanNode, time_dimension_spec: TimeDimensionSpec
    ) -> JoinToCustomGranularityNode:
        return JoinToCustomGranularityNode(parent_nodes=(parent_node,), time_dimension_spec=time_dimension_spec)

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_JOIN_TO_CUSTOM_GRANULARITY_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_join_to_custom_granularity_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return """Join to Custom Granularity Dataset"""

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (
            DisplayedProperty("time_dimension_spec", self.time_dimension_spec),
        )

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self.parent_nodes[0]

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return isinstance(other_node, self.__class__) and other_node.time_dimension_spec == self.time_dimension_spec

    def with_new_parents(  # noqa: D102
        self, new_parent_nodes: Sequence[DataflowPlanNode]
    ) -> JoinToCustomGranularityNode:
        assert len(new_parent_nodes) == 1, "JoinToCustomGranularity accepts exactly one parent node."
        return JoinToCustomGranularityNode.create(
            parent_node=new_parent_nodes[0],
            time_dimension_spec=self.time_dimension_spec,
        )
