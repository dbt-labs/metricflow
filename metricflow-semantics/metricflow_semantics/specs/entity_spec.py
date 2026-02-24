from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from dbt_semantic_interfaces.references import EntityReference
from typing_extensions import override

from metricflow_semantics.specs.instance_spec import InstanceSpecVisitor, LinkableInstanceSpec
from metricflow_semantics.toolkit.visitor import VisitorOutputT


@dataclass(frozen=True)
class EntitySpec(LinkableInstanceSpec, SerializableDataclass):  # noqa: D101
    @staticmethod
    def create_from_element_name(element_name: str) -> EntitySpec:  # noqa: D102
        return EntitySpec(element_name=element_name, entity_links=())

    @staticmethod
    def create_from_reference(entity_reference: EntityReference) -> EntitySpec:  # noqa: D102
        return EntitySpec(element_name=entity_reference.element_name, entity_links=())

    @property
    def without_first_entity_link(self) -> EntitySpec:  # noqa: D102
        assert len(self.entity_links) > 0, f"Spec does not have any entity links: {self}"
        return EntitySpec(element_name=self.element_name, entity_links=self.entity_links[1:])

    @property
    def without_entity_links(self) -> EntitySpec:  # noqa: D102
        return EntitySpec.create_from_element_name(self.element_name)

    @property
    def as_linkless_prefix(self) -> Tuple[EntityReference, ...]:
        """Creates tuple of linkless entities that could be included in the entity_links of another spec.

        e.g. as a prefix to a DimensionSpec's entity links to when a join is occurring via this entity
        """
        return (EntityReference(element_name=self.element_name),) + self.entity_links

    @property
    def reference(self) -> EntityReference:  # noqa: D102
        return EntityReference(element_name=self.element_name)

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_entity_spec(self)

    def with_entity_prefix(self, entity_prefix: EntityReference) -> EntitySpec:  # noqa: D102
        return EntitySpec(element_name=self.element_name, entity_links=(entity_prefix,) + self.entity_links)

    @override
    def with_alias(self, alias: Optional[str]) -> EntitySpec:  # noqa: D102
        return EntitySpec(element_name=self.element_name, entity_links=self.entity_links, alias=alias)
