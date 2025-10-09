from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Sequence

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.visitor import VisitorOutputT
from typing_extensions import override

from metricflow.dataflow.builder.aggregation_helper import InstanceAliasMapping, NullFillValueMapping
from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass(frozen=True, eq=False)
class AggregateSimpleMetricInputsNode(DataflowPlanNode):
    """A node that aggregates the simple-metric inputs by the associated group by elements.

    In the event that one or more of the aggregated input simple-metric inputs has an alias assigned to it, any output query
    resulting from an operation on this node must apply the alias and transform the simple-metric-input instances accordingly,
    otherwise this join could produce a query with two identically named simple-metric-input columns with, e.g., different
    constraints applied to the simple-metric input.

    The simple-metric-input specs are required for downstream nodes to be aware of any simple-metric inputs with
    user-provided aliases, such as we might encounter with constrained and unconstrained versions of the
    same simple-metric.
    """

    alias_mapping: InstanceAliasMapping
    null_fill_value_mapping: NullFillValueMapping

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1

    @staticmethod
    def create(  # noqa: D102
        parent_node: DataflowPlanNode,
        alias_mapping: InstanceAliasMapping,
        null_fill_value_mapping: NullFillValueMapping,
    ) -> AggregateSimpleMetricInputsNode:
        return AggregateSimpleMetricInputsNode(
            parent_nodes=(parent_node,),
            alias_mapping=alias_mapping,
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
            and self.alias_mapping == other_node.alias_mapping
            and self.null_fill_value_mapping == other_node.null_fill_value_mapping
        )

    def with_new_parents(  # noqa: D102
        self, new_parent_nodes: Sequence[DataflowPlanNode]
    ) -> AggregateSimpleMetricInputsNode:
        assert len(new_parent_nodes) == 1
        return AggregateSimpleMetricInputsNode(
            parent_nodes=tuple(new_parent_nodes),
            alias_mapping=self.alias_mapping,
            null_fill_value_mapping=self.null_fill_value_mapping,
        )

    @cached_property
    @override
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        properties: list[DisplayedProperty] = list(super().displayed_properties)
        properties.append(DisplayedProperty("alias_mapping", self.alias_mapping.element_name_to_alias))
        properties.append(
            DisplayedProperty("null_fill_value_mapping", self.null_fill_value_mapping.element_name_to_null_fill_value)
        )
        return properties
