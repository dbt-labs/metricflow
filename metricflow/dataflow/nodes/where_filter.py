from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.toolkit.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass(frozen=True, eq=False)
class WhereFilterNode(DataflowPlanNode):
    """Remove rows using a WHERE clause.

    Attributes:
        filter_specs: Specifications for the WHERE clause to filter rows.
        always_apply: Indicator if the WHERE clause should always be applied.
    """

    filter_specs: Sequence[WhereFilterSpec]
    always_apply: bool

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1

    @staticmethod
    def create(  # noqa: D102
        parent_node: DataflowPlanNode,
        filter_specs: Sequence[WhereFilterSpec],
        always_apply: bool = False,
    ) -> WhereFilterNode:
        return WhereFilterNode(
            parent_nodes=(parent_node,),
            filter_specs=filter_specs,
            always_apply=always_apply,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_WHERE_CONSTRAINT_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_where_constraint_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        # Can't put the where condition here as it can cause rendering issues when there are SQL bind parameters.
        # e.g. "Filter Output with WHERE: listing__country = :1"
        return "Filter Output with WHERE"

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self.parent_nodes[0]

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        properties = tuple(super().displayed_properties) + tuple(
            DisplayedProperty("filter_spec", filter_spec) for filter_spec in self.filter_specs
        )
        if self.always_apply:
            properties = properties + (DisplayedProperty("All filters always applied:", self.always_apply),)
        return properties

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return (
            isinstance(other_node, self.__class__)
            and tuple(filter_spec.where_sql for filter_spec in other_node.filter_specs)
            == tuple(filter_spec.where_sql for filter_spec in self.filter_specs)
            and tuple(filter_spec.bind_parameters for filter_spec in other_node.filter_specs)
            == tuple(filter_spec.bind_parameters for filter_spec in self.filter_specs)
            and other_node.always_apply == self.always_apply
        )

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> WhereFilterNode:  # noqa: D102
        assert len(new_parent_nodes) == 1
        return WhereFilterNode.create(
            parent_node=new_parent_nodes[0],
            filter_specs=self.filter_specs,
            always_apply=self.always_apply,
        )
