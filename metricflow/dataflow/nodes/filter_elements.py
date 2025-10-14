from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.toolkit.mf_logging.pretty_print import mf_pformat
from metricflow_semantics.toolkit.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass(frozen=True, eq=False)
class FilterElementsNode(DataflowPlanNode):
    """Only passes the listed elements.

    Attributes:
        include_specs: The specs for the elements that it should pass.
        replace_description: Replace the default description with this.
        distinct: If you only want the distinct values for the selected specs.
    """

    include_specs: InstanceSpecSet
    replace_description: Optional[str] = None
    distinct: bool = False

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1

    @staticmethod
    def create(  # noqa: D102
        parent_node: DataflowPlanNode,
        include_specs: InstanceSpecSet,
        replace_description: Optional[str] = None,
        distinct: bool = False,
    ) -> FilterElementsNode:
        return FilterElementsNode(
            parent_nodes=(parent_node,),
            include_specs=include_specs,
            replace_description=replace_description,
            distinct=distinct,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_PASS_FILTER_ELEMENTS_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_filter_elements_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        if self.replace_description:
            return self.replace_description

        column_resolver = DunderColumnAssociationResolver()
        return f"Pass Only Elements: {mf_pformat([column_resolver.resolve_spec(spec).column_name for spec in self.include_specs.all_specs])}"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        additional_properties: Tuple[DisplayedProperty, ...] = ()
        if not self.replace_description:
            additional_properties = tuple(
                DisplayedProperty("include_spec", include_spec) for include_spec in self.include_specs.all_specs
            ) + (
                DisplayedProperty("distinct", self.distinct),
            )
        return tuple(super().displayed_properties) + additional_properties

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self.parent_nodes[0]

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return (
            isinstance(other_node, self.__class__)
            and other_node.include_specs == self.include_specs
            and other_node.distinct == self.distinct
        )

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> FilterElementsNode:  # noqa: D102
        assert len(new_parent_nodes) == 1
        return FilterElementsNode(
            parent_nodes=tuple(new_parent_nodes),
            include_specs=self.include_specs,
            distinct=self.distinct,
            replace_description=self.replace_description,
        )
