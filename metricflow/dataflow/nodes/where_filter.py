from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass(frozen=True, eq=False)
class WhereConstraintNode(DataflowPlanNode):
    """Remove rows using a WHERE clause.

    Attributes:
        where_specs: Specifications for the WHERE clause to filter rows.
        always_apply: Indicator if the WHERE clause should always be applied.
    """

    where_specs: Sequence[WhereFilterSpec]
    always_apply: bool

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1

    @staticmethod
    def create(  # noqa: D102
        parent_node: DataflowPlanNode,
        where_specs: Sequence[WhereFilterSpec],
        always_apply: bool = False,
    ) -> WhereConstraintNode:
        return WhereConstraintNode(
            parent_nodes=(parent_node,),
            where_specs=where_specs,
            always_apply=always_apply,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_WHERE_CONSTRAINT_ID_PREFIX

    @property
    def where(self) -> WhereFilterSpec:
        """Returns the specs for the elements that it should pass."""
        return WhereFilterSpec.merge_iterable(self.where_specs)

    @property
    def input_where_specs(self) -> Sequence[WhereFilterSpec]:
        """Returns the discrete set of input where filter specs for this node.

        This is useful for things like predicate pushdown, where we need to differentiate between individual specs
        for pushdown operations on the filter spec level. We merge them for things like rendering and node comparisons,
        but in some cases we may be able to push down a subset of the input specs for efficiency reasons.
        """
        return self.where_specs

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_where_constraint_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        # Can't put the where condition here as it can cause rendering issues when there are SQL execution parameters.
        # e.g. "Constrain Output with WHERE listing__country = :1"
        return "Constrain Output with WHERE"

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self.parent_nodes[0]

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
        return WhereConstraintNode.create(
            parent_node=new_parent_nodes[0],
            where_specs=self.input_where_specs,
            always_apply=self.always_apply,
        )
