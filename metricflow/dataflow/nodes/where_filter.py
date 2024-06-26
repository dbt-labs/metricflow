from __future__ import annotations

from typing import Sequence

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.specs.spec_classes import WhereFilterSpec
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode, DataflowPlanNodeVisitor


class WhereConstraintNode(DataflowPlanNode):
    """Remove rows using a WHERE clause."""

    def __init__(
        self,
        parent_node: DataflowPlanNode,
        where_specs: Sequence[WhereFilterSpec],
        always_apply: bool = False,
    ) -> None:
        """Initializer.

        WhereConstraintNodes must always have exactly one parent, since they always wrap a single subquery input.

        The always_apply parameter serves as an indicator for a WhereConstraintNode that is added to a plan in order
        to clean up null outputs from a pre-join filter. For example, when doing time spine joins to fill null values
        for metric outputs sometimes that join will result in rows with null values for various dimension attributes.
        By re-applying the filter expression after the join step we will discard those unexpected output rows created
        by the join (rather than the underlying inputs). In this case, we must ensure that the filters defined in this
        node are always applied at the moment this node is processed, regardless of whether or not they've been pushed
        down through the DAG.
        """
        self._where_specs = where_specs
        self.parent_node = parent_node
        self.always_apply = always_apply
        super().__init__(node_id=self.create_unique_id(), parent_nodes=(parent_node,))

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_WHERE_CONSTRAINT_ID_PREFIX

    @property
    def where(self) -> WhereFilterSpec:
        """Returns the specs for the elements that it should pass."""
        return WhereFilterSpec.merge_iterable(self._where_specs)

    @property
    def input_where_specs(self) -> Sequence[WhereFilterSpec]:
        """Returns the discrete set of input where filter specs for this node.

        This is useful for things like predicate pushdown, where we need to differentiate between individual specs
        for pushdown operations on the filter spec level. We merge them for things like rendering and node comparisons,
        but in some cases we may be able to push down a subset of the input specs for efficiency reasons.
        """
        return self._where_specs

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_where_constraint_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        # Can't put the where condition here as it can cause rendering issues when there are SQL execution parameters.
        # e.g. "Constrain Output with WHERE listing__country = :1"
        return "Constrain Output with WHERE"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        properties = tuple(super().displayed_properties) + (DisplayedProperty("where_condition", self.where),)
        if self.always_apply:
            properties = properties + (DisplayedProperty("All filters always applied:", self.always_apply),)
        return properties

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return (
            isinstance(other_node, self.__class__)
            and other_node.where == self.where
            and other_node.always_apply == self.always_apply
        )

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> WhereConstraintNode:  # noqa: D102
        assert len(new_parent_nodes) == 1
        return WhereConstraintNode(
            parent_node=new_parent_nodes[0], where_specs=self.input_where_specs, always_apply=self.always_apply
        )
