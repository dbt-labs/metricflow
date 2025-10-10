from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Sequence, Tuple

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.toolkit.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass
class SpecToAlias:
    """A mapping of an input spec that should be aliased to match an output spec."""

    input_spec: InstanceSpec
    output_spec: InstanceSpec


@dataclass(frozen=True, eq=False)
class AliasSpecsNode(DataflowPlanNode, ABC):
    """Change the columns matching the key specs to match the value specs."""

    change_specs: Tuple[SpecToAlias, ...]

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.change_specs) > 0, "Must have at least one value in change_specs for AliasSpecsNode."

    @staticmethod
    def create(parent_node: DataflowPlanNode, change_specs: Tuple[SpecToAlias, ...]) -> AliasSpecsNode:  # noqa: D102
        return AliasSpecsNode(parent_nodes=(parent_node,), change_specs=change_specs)

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_ALIAS_SPECS_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_alias_specs_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return """Change Column Aliases"""

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (DisplayedProperty("change_specs", self.change_specs),)

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self.parent_nodes[0]

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return isinstance(other_node, self.__class__) and other_node.change_specs == self.change_specs

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> AliasSpecsNode:  # noqa: D102
        assert len(new_parent_nodes) == 1, "AliasSpecsNode accepts exactly one parent node."
        return AliasSpecsNode.create(
            parent_node=new_parent_nodes[0],
            change_specs=self.change_specs,
        )
