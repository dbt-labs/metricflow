from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.specs.order_by_spec import OrderBySpec
from metricflow_semantics.toolkit.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
)
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass(frozen=True, eq=False)
class OrderByLimitNode(DataflowPlanNode):
    """A node that re-orders the input data with a limit.

    Attributes:
        order_by_specs: Describes how to order the incoming data.
        limit: Number of rows to limit.
    """

    order_by_specs: Sequence[OrderBySpec]
    limit: Optional[int]

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1

    @staticmethod
    def create(  # noqa: D102
        order_by_specs: Sequence[OrderBySpec],
        parent_node: DataflowPlanNode,
        limit: Optional[int] = None,
    ) -> OrderByLimitNode:
        return OrderByLimitNode(
            parent_nodes=(parent_node,),
            order_by_specs=tuple(order_by_specs),
            limit=limit,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_ORDER_BY_LIMIT_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_order_by_limit_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return f"Order By {[order_by_spec.instance_spec.dunder_name for order_by_spec in self.order_by_specs]}" + (
            f" Limit {self.limit}" if self.limit else ""
        )

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return (
            tuple(super().displayed_properties)
            + tuple(DisplayedProperty("order_by_spec", order_by_spec) for order_by_spec in self.order_by_specs)
            + (DisplayedProperty("limit", str(self.limit)),)
        )

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self.parent_nodes[0]

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return (
            isinstance(other_node, self.__class__)
            and other_node.order_by_specs == self.order_by_specs
            and other_node.limit == self.limit
        )

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> OrderByLimitNode:  # noqa: D102
        assert len(new_parent_nodes) == 1

        return OrderByLimitNode.create(
            parent_node=new_parent_nodes[0],
            order_by_specs=self.order_by_specs,
            limit=self.limit,
        )
