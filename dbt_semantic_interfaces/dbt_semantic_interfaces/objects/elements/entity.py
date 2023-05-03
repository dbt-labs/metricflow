from __future__ import annotations

from typing import Optional, List

from dbt_semantic_interfaces.objects.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
)
from dbt_semantic_interfaces.objects.common import Metadata
from dbt_semantic_interfaces.enum_extension import ExtendedEnum
from dbt_semantic_interfaces.references import (
    EntityReference,
    CompositeSubEntityReference,
)


class EntityType(ExtendedEnum):
    """Defines uniqueness and the extent to which an entity represents the common entity for a data source"""

    FOREIGN = "foreign"
    NATURAL = "natural"
    PRIMARY = "primary"
    UNIQUE = "unique"


class CompositeSubEntity(HashableBaseModel):
    """CompositeSubEntities either describe or reference the entities that comprise a composite entity"""

    name: Optional[str]
    expr: Optional[str]
    ref: Optional[str]

    @property
    def reference(self) -> CompositeSubEntityReference:  # noqa: D
        assert self.name, f"The element name should have been set during model transformation. Got {self}"
        return CompositeSubEntityReference(element_name=self.name)


class Entity(HashableBaseModel, ModelWithMetadataParsing):
    """Describes a entity"""

    name: str
    description: Optional[str]
    type: EntityType
    role: Optional[str]
    entities: List[CompositeSubEntity] = []
    expr: Optional[str] = None
    metadata: Optional[Metadata] = None

    @property
    def is_composite(self) -> bool:  # noqa: D
        return self.entities is not None and len(self.entities) > 0

    @property
    def reference(self) -> EntityReference:  # noqa: D
        return EntityReference(element_name=self.name)

    @property
    def is_linkable_entity_type(self) -> bool:
        """Indicates whether or not this entity can be used as a linkable entity type for joins

        That is, can you use the entity as a linkable element in multi-hop dundered syntax. For example,
        the country dimension in the listings data source can be linked via listing__country, because listing
        is the primary key.

        At the moment, you may only request things accessible via primary, unique, or natural keys, with natural
        keys reserved for SCD Type II style data sources.
        """
        return self.type in (EntityType.PRIMARY, EntityType.UNIQUE, EntityType.NATURAL)
