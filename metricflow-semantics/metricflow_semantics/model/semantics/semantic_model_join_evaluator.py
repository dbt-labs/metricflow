from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional

from dbt_semantic_interfaces.protocols.entity import EntityType
from dbt_semantic_interfaces.references import (
    EntityReference,
    SemanticModelElementReference,
    SemanticModelReference,
)

if TYPE_CHECKING:
    from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup

MAX_JOIN_HOPS = 2


@dataclass(frozen=True)
class SemanticModelEntityJoinType:
    """Describe a type of join between semantic models where entities are of the listed types."""

    left_entity_type: EntityType
    right_entity_type: EntityType


@dataclass(frozen=True)
class SemanticModelEntityJoin:
    """How to join one semantic model onto another, using a specific entity and join type."""

    right_semantic_model_reference: SemanticModelReference
    entity_reference: EntityReference
    join_type: SemanticModelEntityJoinType


@dataclass(frozen=True)
class SemanticModelLink:
    """The valid join path to link two semantic models. Might include multiple joins."""

    left_semantic_model_reference: SemanticModelReference
    join_path: List[SemanticModelEntityJoin]


class SemanticModelJoinEvaluator:
    """Checks to see if a join between two semantic models should be allowed."""

    # Valid joins are the non-fanout joins.
    _VALID_ENTITY_JOINS = (
        SemanticModelEntityJoinType(left_entity_type=EntityType.PRIMARY, right_entity_type=EntityType.NATURAL),
        SemanticModelEntityJoinType(left_entity_type=EntityType.PRIMARY, right_entity_type=EntityType.PRIMARY),
        SemanticModelEntityJoinType(left_entity_type=EntityType.PRIMARY, right_entity_type=EntityType.UNIQUE),
        SemanticModelEntityJoinType(left_entity_type=EntityType.UNIQUE, right_entity_type=EntityType.NATURAL),
        SemanticModelEntityJoinType(left_entity_type=EntityType.UNIQUE, right_entity_type=EntityType.PRIMARY),
        SemanticModelEntityJoinType(left_entity_type=EntityType.UNIQUE, right_entity_type=EntityType.UNIQUE),
        SemanticModelEntityJoinType(left_entity_type=EntityType.FOREIGN, right_entity_type=EntityType.NATURAL),
        SemanticModelEntityJoinType(left_entity_type=EntityType.FOREIGN, right_entity_type=EntityType.PRIMARY),
        SemanticModelEntityJoinType(left_entity_type=EntityType.FOREIGN, right_entity_type=EntityType.UNIQUE),
        SemanticModelEntityJoinType(left_entity_type=EntityType.NATURAL, right_entity_type=EntityType.PRIMARY),
        SemanticModelEntityJoinType(left_entity_type=EntityType.NATURAL, right_entity_type=EntityType.UNIQUE),
    )

    _INVALID_ENTITY_JOINS = (
        SemanticModelEntityJoinType(left_entity_type=EntityType.PRIMARY, right_entity_type=EntityType.FOREIGN),
        SemanticModelEntityJoinType(left_entity_type=EntityType.UNIQUE, right_entity_type=EntityType.FOREIGN),
        SemanticModelEntityJoinType(left_entity_type=EntityType.FOREIGN, right_entity_type=EntityType.FOREIGN),
        SemanticModelEntityJoinType(left_entity_type=EntityType.NATURAL, right_entity_type=EntityType.FOREIGN),
        # Natural -> Natural joins are not allowed due to hidden fanout or missing value concerns with
        # multiple validity windows in play
        SemanticModelEntityJoinType(left_entity_type=EntityType.NATURAL, right_entity_type=EntityType.NATURAL),
    )

    def __init__(self, semantic_model_lookup: SemanticModelLookup) -> None:  # noqa: D107
        self._semantic_model_lookup = semantic_model_lookup

    def get_valid_semantic_model_entity_join_type(
        self,
        left_semantic_model_reference: SemanticModelReference,
        right_semantic_model_reference: SemanticModelReference,
        on_entity_reference: EntityReference,
    ) -> Optional[SemanticModelEntityJoinType]:
        """Get valid join type used to join semantic models on given entity, if exists."""
        left_entity = self._semantic_model_lookup.get_entity_in_semantic_model(
            SemanticModelElementReference.create_from_references(left_semantic_model_reference, on_entity_reference)
        )

        right_entity = self._semantic_model_lookup.get_entity_in_semantic_model(
            SemanticModelElementReference.create_from_references(right_semantic_model_reference, on_entity_reference)
        )
        if left_entity is None or right_entity is None:
            return None

        left_semantic_model = self._semantic_model_lookup.get_by_reference(left_semantic_model_reference)
        right_semantic_model = self._semantic_model_lookup.get_by_reference(right_semantic_model_reference)
        assert left_semantic_model, "Type refinement. If you see this error something has refactored wrongly"
        assert right_semantic_model, "Type refinement. If you see this error something has refactored wrongly"

        if left_semantic_model.has_validity_dimensions and right_semantic_model.has_validity_dimensions:
            # We cannot join two semantic models with validity dimensions due to concerns with unexpected fanout
            # due to the key structure of these semantic models. Applying multi-stage validity window filters can
            # also lead to unexpected removal of interim join keys. Note this will need to be updated if we enable
            # simple-metric inputs in such semantic models, since those will need to be converted to a different type of semantic model
            # to support measure computation.
            return None

        if right_entity.type is EntityType.NATURAL:
            if not right_semantic_model.has_validity_dimensions:
                # There is no way to refine this to a single row per key, so we cannot support this join
                return None

        join_type = SemanticModelEntityJoinType(left_entity.type, right_entity.type)

        if join_type in SemanticModelJoinEvaluator._VALID_ENTITY_JOINS:
            return join_type
        elif join_type in SemanticModelJoinEvaluator._INVALID_ENTITY_JOINS:
            return None

        raise RuntimeError(f"Join type not handled: {join_type}")

    def is_valid_semantic_model_join(
        self,
        left_semantic_model_reference: SemanticModelReference,
        right_semantic_model_reference: SemanticModelReference,
        on_entity_reference: EntityReference,
    ) -> bool:
        """Return true if we should allow a join with the given parameters to resolve a query."""
        return (
            self.get_valid_semantic_model_entity_join_type(
                left_semantic_model_reference=left_semantic_model_reference,
                right_semantic_model_reference=right_semantic_model_reference,
                on_entity_reference=on_entity_reference,
            )
            is not None
        )
