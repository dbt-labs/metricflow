from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from dbt_semantic_interfaces.pretty_print import pformat_big_objects
from dbt_semantic_interfaces.protocols.entity import EntityType
from dbt_semantic_interfaces.references import (
    EntityReference,
    SemanticModelElementReference,
    SemanticModelReference,
)

from metricflow.instances import EntityInstance, InstanceSet
from metricflow.protocols.semantics import SemanticModelAccessor

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

    def __init__(self, semantic_model_lookup: SemanticModelAccessor) -> None:  # noqa: D
        self._semantic_model_lookup = semantic_model_lookup

    def get_joinable_semantic_models(
        self, left_semantic_model_reference: SemanticModelReference, include_multi_hop: bool = False
    ) -> Dict[str, SemanticModelLink]:
        """List all semantic models that can join to given semantic model, and the entities to join them."""
        semantic_model_joins: Dict[str, SemanticModelLink] = {}
        self._get_remaining_hops_of_joinable_semantic_models(
            left_semantic_model_reference=left_semantic_model_reference,
            parent_semantic_model_to_join_paths={left_semantic_model_reference: []},
            known_semantic_model_joins=semantic_model_joins,
            join_hops_remaining=(MAX_JOIN_HOPS if include_multi_hop else 1),
        )
        return semantic_model_joins

    def _get_remaining_hops_of_joinable_semantic_models(
        self,
        left_semantic_model_reference: SemanticModelReference,
        parent_semantic_model_to_join_paths: Dict[SemanticModelReference, List[SemanticModelEntityJoin]],
        known_semantic_model_joins: Dict[str, SemanticModelLink],
        join_hops_remaining: int,
    ) -> None:
        assert join_hops_remaining > 0, "No join hops remaining. This is unexpected with proper use of this method."
        for parent_semantic_model_reference, parent_join_path in parent_semantic_model_to_join_paths.items():
            parent_semantic_model = self._semantic_model_lookup.get_by_reference(
                semantic_model_reference=parent_semantic_model_reference
            )
            assert parent_semantic_model is not None

            # We'll get all joinable semantic models in this hop before recursing to ensure we find the most
            # efficient path to each semantic model.
            join_paths_to_visit_next: List[List[SemanticModelEntityJoin]] = []
            for entity in parent_semantic_model.entities:
                entity_reference = EntityReference(element_name=entity.name)
                entity_semantic_models = self._semantic_model_lookup.get_semantic_models_for_entity(
                    entity_reference=entity_reference
                )

                for right_semantic_model in entity_semantic_models:
                    # Check if we've seen this semantic model already
                    if (
                        right_semantic_model.name == left_semantic_model_reference.semantic_model_name
                        or right_semantic_model.name in known_semantic_model_joins
                    ):
                        continue

                    # Check if there is a valid way to join this semantic model to existing join path
                    right_semantic_model_reference = SemanticModelReference(
                        semantic_model_name=right_semantic_model.name
                    )
                    valid_join_type = self.get_valid_semantic_model_entity_join_type(
                        left_semantic_model_reference=parent_semantic_model_reference,
                        right_semantic_model_reference=right_semantic_model_reference,
                        on_entity_reference=entity_reference,
                    )
                    if valid_join_type is None:
                        continue

                    join_path_for_semantic_model = parent_join_path + [
                        SemanticModelEntityJoin(
                            right_semantic_model_reference=right_semantic_model_reference,
                            entity_reference=entity_reference,
                            join_type=valid_join_type,
                        )
                    ]
                    join_paths_to_visit_next.append(join_path_for_semantic_model)
                    known_semantic_model_joins[right_semantic_model_reference.semantic_model_name] = SemanticModelLink(
                        left_semantic_model_reference=left_semantic_model_reference,
                        join_path=join_path_for_semantic_model,
                    )

        join_hops_remaining -= 1
        if not join_hops_remaining:
            return

        right_semantic_models_to_join_paths: Dict[SemanticModelReference, List[SemanticModelEntityJoin]] = {}
        for join_path in join_paths_to_visit_next:
            assert len(join_path) > 0
            right_semantic_models_to_join_paths[join_path[-1].right_semantic_model_reference] = join_path

        self._get_remaining_hops_of_joinable_semantic_models(
            left_semantic_model_reference=left_semantic_model_reference,
            parent_semantic_model_to_join_paths=right_semantic_models_to_join_paths,
            known_semantic_model_joins=known_semantic_model_joins,
            join_hops_remaining=join_hops_remaining,
        )

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
            # measures in such semantic models, since those will need to be converted to a different type of semantic model
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

    @staticmethod
    def _semantic_model_of_entity_in_instance_set(
        instance_set: InstanceSet,
        entity_reference: EntityReference,
    ) -> SemanticModelReference:
        """Return the semantic model where the entity was defined in the instance set."""
        matching_instances: List[EntityInstance] = []
        for entity_instance in instance_set.entity_instances:
            assert len(entity_instance.defined_from) == 1
            if len(entity_instance.spec.entity_links) == 0 and entity_instance.spec.reference == entity_reference:
                matching_instances.append(entity_instance)

        assert len(matching_instances) == 1, (
            f"Not exactly 1 matching entity instances found: {matching_instances} for {entity_reference} in "
            f"{pformat_big_objects(instance_set)}"
        )
        return matching_instances[0].origin_semantic_model_reference.semantic_model_reference

    def is_valid_instance_set_join(
        self,
        left_instance_set: InstanceSet,
        right_instance_set: InstanceSet,
        on_entity_reference: EntityReference,
    ) -> bool:
        """Return true if the instance sets can be joined using the given entity."""
        return self.is_valid_semantic_model_join(
            left_semantic_model_reference=SemanticModelJoinEvaluator._semantic_model_of_entity_in_instance_set(
                instance_set=left_instance_set, entity_reference=on_entity_reference
            ),
            right_semantic_model_reference=SemanticModelJoinEvaluator._semantic_model_of_entity_in_instance_set(
                instance_set=right_instance_set,
                entity_reference=on_entity_reference,
            ),
            on_entity_reference=on_entity_reference,
        )
