from __future__ import annotations

from typing import Optional, Sequence

from dbt_semantic_interfaces.protocols import MetricTimeWindow
from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.specs.spec_classes import (
    ConstantPropertySpec,
    EntitySpec,
    InstanceSpec,
    MeasureSpec,
    TimeDimensionSpec,
)
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode, DataflowPlanNodeVisitor


class JoinConversionEventsNode(DataflowPlanNode):
    """Builds a data set containing successful conversion events."""

    def __init__(
        self,
        base_node: DataflowPlanNode,
        base_time_dimension_spec: TimeDimensionSpec,
        conversion_node: DataflowPlanNode,
        conversion_measure_spec: MeasureSpec,
        conversion_time_dimension_spec: TimeDimensionSpec,
        unique_identifier_keys: Sequence[InstanceSpec],
        entity_spec: EntitySpec,
        window: Optional[MetricTimeWindow] = None,
        constant_properties: Optional[Sequence[ConstantPropertySpec]] = None,
    ) -> None:
        """Constructor.

        Args:
            base_node: node containing dataset for computing base events.
            base_time_dimension_spec: time dimension for the base events to compute against.
            conversion_node: node containing dataset to join base node for computing conversion events.
            conversion_measure_spec: expose this measure in the resulting dataset for aggregation.
            conversion_time_dimension_spec: time dimension for the conversion events to compute against.
            unique_identifier_keys: columns to uniquely identify each conversion event.
            entity_spec: the specific entity in which the conversion is happening for.
            window: time range bound for when a conversion is still considered valid (default: INF).
            constant_properties: optional set of elements (either dimension/entity) to join the base
                                 event to the conversion event.
        """
        self._base_node = base_node
        self._conversion_node = conversion_node
        self._base_time_dimension_spec = base_time_dimension_spec
        self._conversion_measure_spec = conversion_measure_spec
        self._conversion_time_dimension_spec = conversion_time_dimension_spec
        self._unique_identifier_keys = unique_identifier_keys
        self._entity_spec = entity_spec
        self._window = window
        self._constant_properties = constant_properties
        super().__init__(node_id=self.create_unique_id(), parent_nodes=(base_node, conversion_node))

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_JOIN_CONVERSION_EVENTS_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_join_conversion_events_node(self)

    @property
    def base_node(self) -> DataflowPlanNode:  # noqa: D102
        return self._base_node

    @property
    def conversion_node(self) -> DataflowPlanNode:  # noqa: D102
        return self._conversion_node

    @property
    def conversion_measure_spec(self) -> MeasureSpec:  # noqa: D102
        return self._conversion_measure_spec

    @property
    def base_time_dimension_spec(self) -> TimeDimensionSpec:  # noqa: D102
        return self._base_time_dimension_spec

    @property
    def conversion_time_dimension_spec(self) -> TimeDimensionSpec:  # noqa: D102
        return self._conversion_time_dimension_spec

    @property
    def unique_identifier_keys(self) -> Sequence[InstanceSpec]:  # noqa: D102
        return self._unique_identifier_keys

    @property
    def entity_spec(self) -> EntitySpec:  # noqa: D102
        return self._entity_spec

    @property
    def window(self) -> Optional[MetricTimeWindow]:  # noqa: D102
        return self._window

    @property
    def constant_properties(self) -> Optional[Sequence[ConstantPropertySpec]]:  # noqa: D102
        return self._constant_properties

    @property
    def description(self) -> str:  # noqa: D102
        return f"Find conversions for {self.entity_spec.qualified_name} within the range of {f'{self.window.count} {self.window.granularity.value}' if self.window else 'INF'}"

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
            and other_node.conversion_measure_spec == self.conversion_measure_spec
            and other_node.unique_identifier_keys == self.unique_identifier_keys
            and other_node.entity_spec == self.entity_spec
            and other_node.window == self.window
            and other_node.constant_properties == self.constant_properties
        )

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> JoinConversionEventsNode:  # noqa: D102
        assert len(new_parent_nodes) == 2
        return JoinConversionEventsNode(
            base_node=new_parent_nodes[0],
            base_time_dimension_spec=self.base_time_dimension_spec,
            conversion_node=new_parent_nodes[1],
            conversion_measure_spec=self.conversion_measure_spec,
            conversion_time_dimension_spec=self.conversion_time_dimension_spec,
            unique_identifier_keys=self.unique_identifier_keys,
            entity_spec=self.entity_spec,
            window=self.window,
            constant_properties=self.constant_properties,
        )
