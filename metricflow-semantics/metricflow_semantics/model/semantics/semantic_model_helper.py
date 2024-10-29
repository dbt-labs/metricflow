from __future__ import annotations

from typing import Optional, Sequence

from dbt_semantic_interfaces.protocols import Dimension
from dbt_semantic_interfaces.protocols.entity import Entity
from dbt_semantic_interfaces.protocols.measure import Measure
from dbt_semantic_interfaces.protocols.semantic_model import SemanticModel
from dbt_semantic_interfaces.references import (
    EntityReference,
    LinkableElementReference,
    MeasureReference,
)
from dbt_semantic_interfaces.type_enums import EntityType


class SemanticModelHelper:
    """Static helper methods for retrieving items from a semantic model."""

    @staticmethod
    def get_entity_from_semantic_model(
        semantic_model: SemanticModel, entity_reference: LinkableElementReference
    ) -> Entity:
        """Get entity from semantic model."""
        for entity in semantic_model.entities:
            if entity.reference == entity_reference:
                return entity

        raise ValueError(
            f"No entity with name ({entity_reference}) in semantic_model with name ({semantic_model.name})"
        )

    @staticmethod
    def resolved_primary_entity(semantic_model: SemanticModel) -> Optional[EntityReference]:
        """Return the primary entity for dimensions in the model."""
        primary_entity_reference = semantic_model.primary_entity_reference

        entities_with_type_primary = tuple(
            entity for entity in semantic_model.entities if entity.type == EntityType.PRIMARY
        )

        # This should be caught by the validation, but adding a sanity check.
        assert len(entities_with_type_primary) <= 1, f"Found > 1 primary entity in {semantic_model}"
        if primary_entity_reference is not None:
            assert len(entities_with_type_primary) == 0, (
                f"The primary_entity field was set to {primary_entity_reference}, but there are non-zero entities with "
                f"type {EntityType.PRIMARY} in {semantic_model}"
            )
            return primary_entity_reference

        if len(entities_with_type_primary) > 0:
            return entities_with_type_primary[0].reference

        return None

    @staticmethod
    def entity_links_for_local_elements(semantic_model: SemanticModel) -> Sequence[EntityReference]:
        """Return the entity prefix that can be used to access dimensions defined in the semantic model."""
        primary_entity_reference = semantic_model.primary_entity_reference

        possible_entity_links = set()
        if primary_entity_reference is not None:
            possible_entity_links.add(primary_entity_reference)

        for entity in semantic_model.entities:
            if entity.is_linkable_entity_type:
                possible_entity_links.add(entity.reference)

        return sorted(possible_entity_links, key=lambda entity_reference: entity_reference.element_name)

    @staticmethod
    def get_measure_from_semantic_model(semantic_model: SemanticModel, measure_reference: MeasureReference) -> Measure:
        """Get measure from semantic model."""
        for measure in semantic_model.measures:
            if measure.reference == measure_reference:
                return measure

        raise ValueError(
            f"No dimension with name ({measure_reference.element_name}) in semantic_model with name ({semantic_model.name})"
        )

    @staticmethod
    def get_dimension_from_semantic_model(
        semantic_model: SemanticModel, dimension_reference: LinkableElementReference
    ) -> Dimension:
        """Get dimension from semantic model."""
        for dim in semantic_model.dimensions:
            if dim.reference == dimension_reference:
                return dim
        raise ValueError(
            f"No dimension with name ({dimension_reference}) in semantic_model with name ({semantic_model.name})"
        )
