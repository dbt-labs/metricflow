from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Sequence, Tuple

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet
from metricflow_semantics.toolkit.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass(frozen=True, eq=False)
class WhereConstraintNode(DataflowPlanNode):
    """Remove rows using a WHERE clause.

    Attributes:
        where_specs: Specifications for the WHERE clause to filter rows.
        always_apply: Indicator if the WHERE clause should always be applied.
        exposed_simple_metric_names: Before the measure -> simple-metric migration, a column with the same name as
        a measure was present in the `SELECT` associated with this node. Although unsupported, customers may have
        written SQL that depends on this behavior. Post-migration, simple-metric inputs are normally associated with a
        column alias that has a dunder prefix. To replicate the prior behavior, this field can be set with the names
        of the simple-metrics that should be associated with a column name without a dunder prefix. Entity instances
        with the same name as one of the simple metrics specified here will not be available in this node or
        the downstream nodes.
    """

    where_specs: Tuple[WhereFilterSpec, ...]
    always_apply: bool
    exposed_simple_metric_names: Tuple[str, ...]

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1
        assert len(self.exposed_simple_metric_names) == len(set(self.exposed_simple_metric_names))

    @staticmethod
    def create(  # noqa: D102
        parent_node: DataflowPlanNode,
        where_specs: Sequence[WhereFilterSpec],
        always_apply: bool = False,
        exposed_simple_metric_names: Optional[Iterable[str]] = None,
    ) -> WhereConstraintNode:
        return WhereConstraintNode(
            parent_nodes=(parent_node,),
            where_specs=tuple(where_specs),
            always_apply=always_apply,
            exposed_simple_metric_names=tuple(FrozenOrderedSet(exposed_simple_metric_names))
            if exposed_simple_metric_names is not None
            else (),
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
        properties = list(super().displayed_properties)
        properties.append(DisplayedProperty("where_condition", self.where))
        if self.always_apply:
            properties.append(DisplayedProperty("All filters always applied:", self.always_apply))
        if self.exposed_simple_metric_names:
            properties.append(DisplayedProperty("exposed_simple_metric_names", list(self.exposed_simple_metric_names)))
        return properties

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return (
            isinstance(other_node, self.__class__)
            and other_node.where == self.where
            and other_node.always_apply == self.always_apply
            and other_node.exposed_simple_metric_names == self.exposed_simple_metric_names
        )

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> WhereConstraintNode:  # noqa: D102
        assert len(new_parent_nodes) == 1
        return WhereConstraintNode.create(
            parent_node=new_parent_nodes[0],
            where_specs=self.input_where_specs,
            always_apply=self.always_apply,
            exposed_simple_metric_names=self.exposed_simple_metric_names,
        )
