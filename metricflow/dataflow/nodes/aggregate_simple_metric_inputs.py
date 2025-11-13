from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Sequence

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.toolkit.visitor import VisitorOutputT
from typing_extensions import override

from metricflow.dataflow.builder.aggregation_helper import NullFillValueMapping
from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass(frozen=True, eq=False)
class AggregateSimpleMetricInputsNode(DataflowPlanNode):
    """A node that aggregates the simple-metric inputs by the associated group by elements."""

    null_fill_value_mapping: NullFillValueMapping

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1

    @staticmethod
    def create(  # noqa: D102
        parent_node: DataflowPlanNode,
        null_fill_value_mapping: NullFillValueMapping,
    ) -> AggregateSimpleMetricInputsNode:
        return AggregateSimpleMetricInputsNode(
            parent_nodes=(parent_node,),
            null_fill_value_mapping=null_fill_value_mapping,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_AGGREGATE_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_aggregate_simple_metric_inputs_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return """Aggregate Inputs for Simple Metrics"""

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self.parent_nodes[0]

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return (
            isinstance(other_node, self.__class__)
            and self.null_fill_value_mapping == other_node.null_fill_value_mapping
        )

    def with_new_parents(  # noqa: D102
        self, new_parent_nodes: Sequence[DataflowPlanNode]
    ) -> AggregateSimpleMetricInputsNode:
        assert len(new_parent_nodes) == 1
        return AggregateSimpleMetricInputsNode(
            parent_nodes=tuple(new_parent_nodes),
            null_fill_value_mapping=self.null_fill_value_mapping,
        )

    @cached_property
    @override
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        properties: list[DisplayedProperty] = list(super().displayed_properties)
        properties.append(
            DisplayedProperty("null_fill_value_mapping", self.null_fill_value_mapping.element_name_to_null_fill_value)
        )
        return properties
