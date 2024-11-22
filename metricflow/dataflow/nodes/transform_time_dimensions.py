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
class TransformTimeDimensionsNode(DataflowPlanNode, ABC):
    """Change the columns in the parent node to match the requested time dimension specs.

    Args:
        requested_time_dimension_specs: The time dimension specs to match in the parent node and transform.
    """

    requested_time_dimension_specs: Sequence[TimeDimensionSpec]

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert (
            len(self.requested_time_dimension_specs) > 0
        ), "Must have at least one value in requested_time_dimension_specs for TransformTimeDimensionsNode."

    @staticmethod
    def create(  # noqa: D102
        parent_node: DataflowPlanNode, requested_time_dimension_specs: Sequence[TimeDimensionSpec]
    ) -> TransformTimeDimensionsNode:
        return TransformTimeDimensionsNode(
            parent_nodes=(parent_node,), requested_time_dimension_specs=requested_time_dimension_specs
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_JOIN_TO_CUSTOM_GRANULARITY_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_transform_time_dimensions_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return """Transform Time Dimension Columns"""

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (
            DisplayedProperty("requested_time_dimension_specs", self.requested_time_dimension_specs),
        )

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self.parent_nodes[0]

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return (
            isinstance(other_node, self.__class__)
            and other_node.requested_time_dimension_specs == self.requested_time_dimension_specs
        )

    def with_new_parents(  # noqa: D102
        self, new_parent_nodes: Sequence[DataflowPlanNode]
    ) -> TransformTimeDimensionsNode:
        assert len(new_parent_nodes) == 1, "TransformTimeDimensionsNode accepts exactly one parent node."
        return TransformTimeDimensionsNode.create(
            parent_node=new_parent_nodes[0],
            requested_time_dimension_specs=self.requested_time_dimension_specs,
        )
