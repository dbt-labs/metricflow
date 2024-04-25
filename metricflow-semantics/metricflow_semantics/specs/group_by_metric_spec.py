from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from dbt_semantic_interfaces.references import EntityReference, MetricReference
from typing_extensions import override

from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.specs.spec_classes import (
    EntitySpec,
    InstanceSpecSet,
    InstanceSpecVisitor,
    LinkableInstanceSpec,
    MetricSpec,
)
from metricflow_semantics.visitor import VisitorOutputT


@dataclass(frozen=True)
class GroupByMetricSpec(LinkableInstanceSpec, SerializableDataclass):
    """Metric used in group by or where filter."""

    @property
    def without_first_entity_link(self) -> GroupByMetricSpec:  # noqa: D102
        assert len(self.entity_links) > 0, f"Spec does not have any entity links: {self}"
        return GroupByMetricSpec(element_name=self.element_name, entity_links=self.entity_links[1:])

    @property
    def without_entity_links(self) -> GroupByMetricSpec:  # noqa: D102
        return GroupByMetricSpec(element_name=self.element_name, entity_links=())

    @staticmethod
    def from_name(name: str) -> GroupByMetricSpec:  # noqa: D102
        structured_name = StructuredLinkableSpecName.from_name(name)
        return GroupByMetricSpec(
            entity_links=tuple(EntityReference(idl) for idl in structured_name.entity_link_names),
            element_name=structured_name.element_name,
        )

    def __eq__(self, other: Any) -> bool:  # type: ignore[misc] # noqa: D105
        if not isinstance(other, GroupByMetricSpec):
            return False
        return self.element_name == other.element_name and self.entity_links == other.entity_links

    def __hash__(self) -> int:  # noqa: D105
        return hash((self.element_name, self.entity_links))

    @property
    def reference(self) -> MetricReference:  # noqa: D102
        return MetricReference(element_name=self.element_name)

    @property
    @override
    def as_spec_set(self) -> InstanceSpecSet:
        return InstanceSpecSet(group_by_metric_specs=(self,))

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_group_by_metric_spec(self)

    @property
    def query_spec_for_source_node(self) -> MetricFlowQuerySpec:
        """Query spec that can be used to build a source node for this spec in the DFP."""
        return MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name=self.element_name),),
            entity_specs=tuple(EntitySpec.from_name(entity_link.element_name) for entity_link in self.entity_links),
        )
