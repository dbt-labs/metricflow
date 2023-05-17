from __future__ import annotations

from typing import Optional

from dbt_semantic_interfaces.objects.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
)
from dbt_semantic_interfaces.objects.metadata import Metadata
from dbt_semantic_interfaces.enum_extension import ExtendedEnum
from dbt_semantic_interfaces.references import EntityReference


class EntityType(ExtendedEnum):
    """Defines uniqueness and the extent to which an entity represents the common entity for a semantic model"""

    FOREIGN = "foreign"
    NATURAL = "natural"
    PRIMARY = "primary"
    UNIQUE = "unique"


class Entity(HashableBaseModel, ModelWithMetadataParsing):
    """Describes a entity"""

    name: str
    description: Optional[str]
    type: EntityType
    role: Optional[str]
    expr: Optional[str] = None
    metadata: Optional[Metadata] = None

    @property
    def reference(self) -> EntityReference:  # noqa: D
        return EntityReference(element_name=self.name)

    @property
    def is_linkable_entity_type(self) -> bool:
        """Indicates whether or not this entity can be used as a linkable entity type for joins

        That is, can you use the entity as a linkable element in multi-hop dundered syntax. For example,
        the country dimension in the listings semantic model can be linked via listing__country, because listing
        is the primary key.

        At the moment, you may only request things accessible via primary, unique, or natural keys, with natural
        keys reserved for SCD Type II style semantic models.
        """
        return self.type in (EntityType.PRIMARY, EntityType.UNIQUE, EntityType.NATURAL)
