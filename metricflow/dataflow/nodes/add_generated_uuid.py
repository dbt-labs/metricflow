from __future__ import annotations

from typing import Sequence

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode, DataflowPlanNodeVisitor


class AddGeneratedUuidColumnNode(DataflowPlanNode):
    """Adds a UUID column."""

    def __init__(self, parent_node: DataflowPlanNode) -> None:  # noqa: D107
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[parent_node])

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_ADD_UUID_COLUMN_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_add_generated_uuid_column_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return "Adds an internally generated UUID column"

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        assert len(self.parent_nodes) == 1
        return self.parent_nodes[0]

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return super().displayed_properties

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return isinstance(other_node, self.__class__)

    def with_new_parents(  # noqa: D102
        self, new_parent_nodes: Sequence[DataflowPlanNode]
    ) -> AddGeneratedUuidColumnNode:
        assert len(new_parent_nodes) == 1
        return AddGeneratedUuidColumnNode(parent_node=new_parent_nodes[0])
