from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Sequence

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass(frozen=True, eq=False)
class CustomGranularityBoundsNode(DataflowPlanNode, ABC):
    """Calculate the start and end of a custom granularity period and each row number within that period."""

    custom_granularity_name: str

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1

    @staticmethod
    def create(  # noqa: D102
        parent_node: DataflowPlanNode, custom_granularity_name: str
    ) -> CustomGranularityBoundsNode:
        return CustomGranularityBoundsNode(parent_nodes=(parent_node,), custom_granularity_name=custom_granularity_name)

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_CUSTOM_GRANULARITY_BOUNDS_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_custom_granularity_bounds_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return """Calculate Custom Granularity Bounds"""

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (
            DisplayedProperty("custom_granularity_name", self.custom_granularity_name),
        )

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self.parent_nodes[0]

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return (
            isinstance(other_node, self.__class__)
            and other_node.custom_granularity_name == self.custom_granularity_name
        )

    def with_new_parents(  # noqa: D102
        self, new_parent_nodes: Sequence[DataflowPlanNode]
    ) -> CustomGranularityBoundsNode:
        assert len(new_parent_nodes) == 1
        return CustomGranularityBoundsNode.create(
            parent_node=new_parent_nodes[0], custom_granularity_name=self.custom_granularity_name
        )
