from __future__ import annotations

from typing import Optional, Sequence, Union

from metricflow.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow.dag.mf_dag import DisplayedProperty
from metricflow.dataflow.dataflow_plan import (
    BaseOutput,
    ComputedMetricsOutput,
    DataflowPlanNode,
    DataflowPlanNodeVisitor,
)
from metricflow.specs.specs import OrderBySpec
from metricflow.visitor import VisitorOutputT


class OrderByLimitNode(ComputedMetricsOutput):
    """A node that re-orders the input data with a limit."""

    def __init__(
        self,
        order_by_specs: Sequence[OrderBySpec],
        parent_node: Union[BaseOutput, ComputedMetricsOutput],
        limit: Optional[int] = None,
    ) -> None:
        """Constructor.

        Args:
            order_by_specs: describes how to order the incoming data.
            limit: number of rows to limit.
            parent_node: self-explanatory.
        """
        self._order_by_specs = tuple(order_by_specs)
        self._limit = limit
        self._parent_node = parent_node
        super().__init__(node_id=self.create_unique_id(), parent_nodes=(self._parent_node,))

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D
        return StaticIdPrefix.DATAFLOW_NODE_ORDER_BY_LIMIT_ID_PREFIX

    @property
    def order_by_specs(self) -> Sequence[OrderBySpec]:
        """The elements that this node should order the input data."""
        return self._order_by_specs

    @property
    def limit(self) -> Optional[int]:
        """The number of rows to limit by."""
        return self._limit

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_order_by_limit_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return f"Order By {[order_by_spec.instance_spec.qualified_name for order_by_spec in self._order_by_specs]}" + (
            f" Limit {self._limit}" if self.limit else ""
        )

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D
        return (
            tuple(super().displayed_properties)
            + tuple(DisplayedProperty("order_by_spec", order_by_spec) for order_by_spec in self._order_by_specs)
            + (DisplayedProperty("limit", str(self.limit)),)
        )

    @property
    def parent_node(self) -> Union[BaseOutput, ComputedMetricsOutput]:  # noqa: D
        return self._parent_node

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        return (
            isinstance(other_node, self.__class__)
            and other_node.order_by_specs == self.order_by_specs
            and other_node.limit == self.limit
        )

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> OrderByLimitNode:  # noqa: D
        assert len(new_parent_nodes) == 1

        return OrderByLimitNode(
            parent_node=new_parent_nodes[0],
            order_by_specs=self.order_by_specs,
            limit=self.limit,
        )
