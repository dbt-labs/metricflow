from __future__ import annotations

from metricflow_semantic_interfaces.enum_extension import ExtendedEnum


class EntityType(ExtendedEnum):
    """Defines uniqueness and the extent to which an entity represents the common entity for a semantic model."""

    FOREIGN = "foreign"
    NATURAL = "natural"
    PRIMARY = "primary"
    UNIQUE = "unique"
