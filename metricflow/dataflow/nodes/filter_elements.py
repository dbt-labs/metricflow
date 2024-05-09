from __future__ import annotations

from typing import Optional, Sequence, Tuple

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.mf_logging.pretty_print import mf_pformat
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode, DataflowPlanNodeVisitor


class FilterElementsNode(DataflowPlanNode):
    """Only passes the listed elements."""

    def __init__(  # noqa: D107
        self,
        parent_node: DataflowPlanNode,
        include_specs: InstanceSpecSet,
        replace_description: Optional[str] = None,
        distinct: bool = False,
    ) -> None:
        self._include_specs = include_specs
        self._replace_description = replace_description
        self._parent_node = parent_node
        self._distinct = distinct
        super().__init__(node_id=self.create_unique_id(), parent_nodes=(parent_node,))

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_PASS_FILTER_ELEMENTS_ID_PREFIX

    @property
    def include_specs(self) -> InstanceSpecSet:
        """Returns the specs for the elements that it should pass."""
        return self._include_specs

    @property
    def distinct(self) -> bool:
        """True if you only want the distinct values for the selected specs."""
        return self._distinct

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_filter_elements_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        if self._replace_description:
            return self._replace_description

        return f"Pass Only Elements: {mf_pformat([x.qualified_name for x in self._include_specs.all_specs])}"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        additional_properties: Tuple[DisplayedProperty, ...] = ()
        if not self._replace_description:
            additional_properties = tuple(
                DisplayedProperty("include_spec", include_spec) for include_spec in self._include_specs.all_specs
            ) + (
                DisplayedProperty("distinct", self._distinct),
            )
        return tuple(super().displayed_properties) + additional_properties

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self._parent_node

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return (
            isinstance(other_node, self.__class__)
            and other_node.include_specs == self.include_specs
            and other_node.distinct == self.distinct
        )

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> FilterElementsNode:  # noqa: D102
        assert len(new_parent_nodes) == 1
        return FilterElementsNode(
            parent_node=new_parent_nodes[0],
            include_specs=self.include_specs,
            distinct=self.distinct,
            replace_description=self._replace_description,
        )
