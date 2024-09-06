from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from dbt_semantic_interfaces.references import ElementReference, EntityReference


@dataclass(frozen=True, order=True)
class AttributeReference(ElementReference):
    pass


@dataclass(frozen=True, order=True)
class AssociativeEntityReference(ElementReference):
    @staticmethod
    def create(entity_references: Sequence[EntityReference]) -> AssociativeEntityReference:
        return AssociativeEntityReference(
            element_name=str(tuple(entity_reference.element_name for entity_reference in entity_references))
        )
