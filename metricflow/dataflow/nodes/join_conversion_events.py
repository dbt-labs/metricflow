from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

from dbt_semantic_interfaces.protocols import MetricTimeWindow
from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.specs.constant_property_spec import ConstantPropertySpec
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.specs.simple_metric_input_spec import SimpleMetricInputSpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.toolkit.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass(frozen=True, eq=False)
class JoinConversionEventsNode(DataflowPlanNode):
    """Builds a data set containing successful conversion events.

    Attributes:
        base_node: node containing dataset for computing base events.
        base_time_dimension_spec: time dimension for the base events to compute against.
        conversion_node: node containing dataset to join base node for computing conversion events.
        conversion_input_metric_spec: expose this simple metric in the resulting dataset for aggregation.
        conversion_time_dimension_spec: time dimension for the conversion events to compute against.
        unique_identifier_keys: columns to uniquely identify each conversion event.
        entity_spec: the specific entity in which the conversion is happening for.
        window: time range bound for when a conversion is still considered valid (default: INF).
        constant_properties: optional set of elements (either dimension/entity) to join the base
                             event to the conversion event.
    """

    base_node: DataflowPlanNode
    base_time_dimension_spec: TimeDimensionSpec
    conversion_node: DataflowPlanNode
    conversion_input_metric_spec: SimpleMetricInputSpec
    conversion_time_dimension_spec: TimeDimensionSpec
    unique_identifier_keys: Tuple[InstanceSpec, ...]
    entity_spec: EntitySpec
    window: Optional[MetricTimeWindow]
    constant_properties: Optional[Tuple[ConstantPropertySpec, ...]]

    @staticmethod
    def create(  # noqa: D102
        base_node: DataflowPlanNode,
        base_time_dimension_spec: TimeDimensionSpec,
        conversion_node: DataflowPlanNode,
        conversion_simple_metric_input_spec: SimpleMetricInputSpec,
        conversion_time_dimension_spec: TimeDimensionSpec,
        unique_identifier_keys: Sequence[InstanceSpec],
        entity_spec: EntitySpec,
        window: Optional[MetricTimeWindow] = None,
        constant_properties: Optional[Sequence[ConstantPropertySpec]] = None,
    ) -> JoinConversionEventsNode:
        return JoinConversionEventsNode(
            parent_nodes=(base_node, conversion_node),
            base_node=base_node,
            base_time_dimension_spec=base_time_dimension_spec,
            conversion_node=conversion_node,
            conversion_input_metric_spec=conversion_simple_metric_input_spec,
            conversion_time_dimension_spec=conversion_time_dimension_spec,
            unique_identifier_keys=tuple(unique_identifier_keys),
            entity_spec=entity_spec,
            window=window,
            constant_properties=tuple(constant_properties) if constant_properties is not None else None,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_JOIN_CONVERSION_EVENTS_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_join_conversion_events_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return f"Find conversions for {self.entity_spec.dunder_name} within the range of {f'{self.window.count} {self.window.granularity}' if self.window else 'INF'}"

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return (
            tuple(super().displayed_properties)
            + (
                DisplayedProperty("base_time_dimension_spec", self.base_time_dimension_spec),
                DisplayedProperty("conversion_time_dimension_spec", self.conversion_time_dimension_spec),
                DisplayedProperty("entity_spec", self.entity_spec),
                DisplayedProperty("window", self.window),
            )
            + tuple(DisplayedProperty("unique_key_specs", unique_spec) for unique_spec in self.unique_identifier_keys)
            + tuple(
                DisplayedProperty("constant_property", constant_property)
                for constant_property in self.constant_properties or []
            )
        )

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return (
            isinstance(other_node, self.__class__)
            and other_node.base_time_dimension_spec == self.base_time_dimension_spec
            and other_node.conversion_time_dimension_spec == self.conversion_time_dimension_spec
            and other_node.conversion_input_metric_spec == self.conversion_input_metric_spec
            and other_node.unique_identifier_keys == self.unique_identifier_keys
            and other_node.entity_spec == self.entity_spec
            and other_node.window == self.window
            and other_node.constant_properties == self.constant_properties
        )

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> JoinConversionEventsNode:  # noqa: D102
        assert len(new_parent_nodes) == 2
        return JoinConversionEventsNode(
            parent_nodes=tuple(new_parent_nodes),
            base_node=new_parent_nodes[0],
            base_time_dimension_spec=self.base_time_dimension_spec,
            conversion_node=new_parent_nodes[1],
            conversion_input_metric_spec=self.conversion_input_metric_spec,
            conversion_time_dimension_spec=self.conversion_time_dimension_spec,
            unique_identifier_keys=self.unique_identifier_keys,
            entity_spec=self.entity_spec,
            window=self.window,
            constant_properties=self.constant_properties,
        )
