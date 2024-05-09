from __future__ import annotations

from typing import Sequence

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.specs.spec_classes import WhereFilterSpec
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode, DataflowPlanNodeVisitor


class WhereConstraintNode(DataflowPlanNode):
    """Remove rows using a WHERE clause."""

    def __init__(  # noqa: D107
        self,
        parent_node: DataflowPlanNode,
        where_constraint: WhereFilterSpec,
    ) -> None:
        self._where = where_constraint
        self.parent_node = parent_node
        super().__init__(node_id=self.create_unique_id(), parent_nodes=(parent_node,))

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_WHERE_CONSTRAINT_ID_PREFIX

    @property
    def where(self) -> WhereFilterSpec:
        """Returns the specs for the elements that it should pass."""
        return self._where

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_where_constraint_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        # Can't put the where condition here as it can cause rendering issues when there are SQL execution parameters.
        # e.g. "Constrain Output with WHERE listing__country = :1"
        return "Constrain Output with WHERE"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (DisplayedProperty("where_condition", self.where),)

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return isinstance(other_node, self.__class__) and other_node.where == self.where

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> WhereConstraintNode:  # noqa: D102
        assert len(new_parent_nodes) == 1
        return WhereConstraintNode(
            parent_node=new_parent_nodes[0],
            where_constraint=self.where,
        )
