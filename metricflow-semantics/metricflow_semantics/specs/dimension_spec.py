from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from dbt_semantic_interfaces.references import DimensionReference, EntityReference
from typing_extensions import override

from metricflow_semantics.model.semantics.linkable_element import ElementPathKey, LinkableElementType
from metricflow_semantics.specs.instance_spec import InstanceSpecVisitor, LinkableInstanceSpec
from metricflow_semantics.visitor import VisitorOutputT


@dataclass(frozen=True)
class DimensionSpec(LinkableInstanceSpec, SerializableDataclass):  # noqa: D101
    element_name: str
    entity_links: Tuple[EntityReference, ...]

    @property
    def without_first_entity_link(self) -> DimensionSpec:  # noqa: D102
        assert len(self.entity_links) > 0, f"Spec does not have any entity links: {self}"
        return DimensionSpec(element_name=self.element_name, entity_links=self.entity_links[1:])

    @property
    def without_entity_links(self) -> DimensionSpec:  # noqa: D102
        return DimensionSpec(element_name=self.element_name, entity_links=())

    @staticmethod
    def from_linkable(spec: LinkableInstanceSpec) -> DimensionSpec:  # noqa: D102
        return DimensionSpec(element_name=spec.element_name, entity_links=spec.entity_links)

    @property
    def reference(self) -> DimensionReference:  # noqa: D102
        return DimensionReference(element_name=self.element_name)

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_dimension_spec(self)

    @property
    @override
    def element_path_key(self) -> ElementPathKey:
        return ElementPathKey(
            element_name=self.element_name, element_type=LinkableElementType.DIMENSION, entity_links=self.entity_links
        )

    def with_entity_prefix(self, entity_prefix: EntityReference) -> DimensionSpec:  # noqa: D102
        return DimensionSpec(element_name=self.element_name, entity_links=(entity_prefix,) + self.entity_links)
