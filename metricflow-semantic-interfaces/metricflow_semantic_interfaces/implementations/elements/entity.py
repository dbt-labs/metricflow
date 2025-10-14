from __future__ import annotations

from typing import Optional

from metricflow_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
)
from metricflow_semantic_interfaces.implementations.element_config import (
    PydanticSemanticLayerElementConfig,
)
from metricflow_semantic_interfaces.implementations.metadata import PydanticMetadata
from metricflow_semantic_interfaces.references import EntityReference
from metricflow_semantic_interfaces.type_enums import EntityType


class PydanticEntity(HashableBaseModel, ModelWithMetadataParsing):
    """Describes a entity."""

    name: str
    description: Optional[str]
    type: EntityType
    role: Optional[str]
    expr: Optional[str] = None
    metadata: Optional[PydanticMetadata] = None
    label: Optional[str] = None
    config: Optional[PydanticSemanticLayerElementConfig]

    @property
    def reference(self) -> EntityReference:  # noqa: D102
        return EntityReference(element_name=self.name)

    @property
    def is_linkable_entity_type(self) -> bool:  # noqa: D102
        return self.type in (EntityType.PRIMARY, EntityType.UNIQUE, EntityType.NATURAL)
