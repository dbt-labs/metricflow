from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Optional

from metricflow.instances import EntityReference, EntityElementReference, IdentifierInstance, InstanceSet
from dbt.contracts.graph.identifiers import IdentifierType
from metricflow.object_utils import pformat_big_objects
from metricflow.protocols.semantics import EntitySemanticsAccessor
from dbt.semantic.references import IdentifierReference

MAX_JOIN_HOPS = 2


@dataclass(frozen=True)
class EntityIdentifierJoinType:
    """Describe a type of join between entities where identifiers are of the listed types."""

    left_identifier_type: IdentifierType
    right_identifier_type: IdentifierType


@dataclass(frozen=True)
class EntityIdentifierJoin:
    """How to join one entity onto another, using a specific identifer and join type."""

    right_entity_reference: EntityReference
    identifier_reference: IdentifierReference
    join_type: EntityIdentifierJoinType


@dataclass(frozen=True)
class EntityLink:
    """The valid join path to link two entities. Might include multiple joins."""

    left_entity_reference: EntityReference
    join_path: List[EntityIdentifierJoin]


class EntityJoinEvaluator:
    """Checks to see if a join between two entities should be allowed."""

    # Valid joins are the non-fanout joins.
    _VALID_IDENTIFIER_JOINS = (
        EntityIdentifierJoinType(
            left_identifier_type=IdentifierType.PRIMARY, right_identifier_type=IdentifierType.NATURAL
        ),
        EntityIdentifierJoinType(
            left_identifier_type=IdentifierType.PRIMARY, right_identifier_type=IdentifierType.PRIMARY
        ),
        EntityIdentifierJoinType(
            left_identifier_type=IdentifierType.PRIMARY, right_identifier_type=IdentifierType.UNIQUE
        ),
        EntityIdentifierJoinType(
            left_identifier_type=IdentifierType.UNIQUE, right_identifier_type=IdentifierType.NATURAL
        ),
        EntityIdentifierJoinType(
            left_identifier_type=IdentifierType.UNIQUE, right_identifier_type=IdentifierType.PRIMARY
        ),
        EntityIdentifierJoinType(
            left_identifier_type=IdentifierType.UNIQUE, right_identifier_type=IdentifierType.UNIQUE
        ),
        EntityIdentifierJoinType(
            left_identifier_type=IdentifierType.FOREIGN, right_identifier_type=IdentifierType.NATURAL
        ),
        EntityIdentifierJoinType(
            left_identifier_type=IdentifierType.FOREIGN, right_identifier_type=IdentifierType.PRIMARY
        ),
        EntityIdentifierJoinType(
            left_identifier_type=IdentifierType.FOREIGN, right_identifier_type=IdentifierType.UNIQUE
        ),
        EntityIdentifierJoinType(
            left_identifier_type=IdentifierType.NATURAL, right_identifier_type=IdentifierType.PRIMARY
        ),
        EntityIdentifierJoinType(
            left_identifier_type=IdentifierType.NATURAL, right_identifier_type=IdentifierType.UNIQUE
        ),
    )

    _INVALID_IDENTIFIER_JOINS = (
        EntityIdentifierJoinType(
            left_identifier_type=IdentifierType.PRIMARY, right_identifier_type=IdentifierType.FOREIGN
        ),
        EntityIdentifierJoinType(
            left_identifier_type=IdentifierType.UNIQUE, right_identifier_type=IdentifierType.FOREIGN
        ),
        EntityIdentifierJoinType(
            left_identifier_type=IdentifierType.FOREIGN, right_identifier_type=IdentifierType.FOREIGN
        ),
        EntityIdentifierJoinType(
            left_identifier_type=IdentifierType.NATURAL, right_identifier_type=IdentifierType.FOREIGN
        ),
        # Natural -> Natural joins are not allowed due to hidden fanout or missing value concerns with
        # multiple validity windows in play
        EntityIdentifierJoinType(
            left_identifier_type=IdentifierType.NATURAL, right_identifier_type=IdentifierType.NATURAL
        ),
    )

    def __init__(self, entity_semantics: EntitySemanticsAccessor) -> None:  # noqa: D
        self._entity_semantics = entity_semantics

    def get_joinable_entities(
        self, left_entity_reference: EntityReference, include_multi_hop: bool = False
    ) -> Dict[str, EntityLink]:
        """List all entities that can join to given entity, and the identifiers to join them."""
        entity_joins: Dict[str, EntityLink] = {}
        self._get_remaining_hops_of_joinable_entities(
            left_entity_reference=left_entity_reference,
            parent_entity_to_join_paths={left_entity_reference: []},
            known_entity_joins=entity_joins,
            join_hops_remaining=(MAX_JOIN_HOPS if include_multi_hop else 1),
        )
        return entity_joins

    def _get_remaining_hops_of_joinable_entities(
        self,
        left_entity_reference: EntityReference,
        parent_entity_to_join_paths: Dict[EntityReference, List[EntityIdentifierJoin]],
        known_entity_joins: Dict[str, EntityLink],
        join_hops_remaining: int,
    ) -> None:
        assert join_hops_remaining > 0, "No join hops remaining. This is unexpected with proper use of this method."
        for parent_entity_reference, parent_join_path in parent_entity_to_join_paths.items():
            parent_entity = self._entity_semantics.get_by_reference(
                entity_reference=parent_entity_reference
            )
            assert parent_entity is not None

            # We'll get all joinable entities in this hop before recursing to ensure we find the most
            # efficient path to each entity.
            join_paths_to_visit_next: List[List[EntityIdentifierJoin]] = []
            for identifier in parent_entity.identifiers:
                identifier_reference = IdentifierReference(element_name=identifier.name)
                identifier_entities = self._entity_semantics.get_entities_for_identifier(
                    identifier_reference=identifier_reference
                )

                for right_entity in identifier_entities:
                    # Check if we've seen this entity already
                    if (
                        right_entity.name == left_entity_reference.entity_name
                        or right_entity.name in known_entity_joins
                    ):
                        continue

                    # Check if there is a valid way to join this entity to existing join path
                    right_entity_reference = EntityReference(entity_name=right_entity.name)
                    valid_join_type = self.get_valid_entity_identifier_join_type(
                        left_entity_reference=parent_entity_reference,
                        right_entity_reference=right_entity_reference,
                        on_identifier_reference=identifier_reference,
                    )
                    if valid_join_type is None:
                        continue

                    join_path_for_entity = parent_join_path + [
                        EntityIdentifierJoin(
                            right_entity_reference=right_entity_reference,
                            identifier_reference=identifier_reference,
                            join_type=valid_join_type,
                        )
                    ]
                    join_paths_to_visit_next.append(join_path_for_entity)
                    known_entity_joins[right_entity_reference.entity_name] = EntityLink(
                        left_entity_reference=left_entity_reference, join_path=join_path_for_entity
                    )

        join_hops_remaining -= 1
        if not join_hops_remaining:
            return

        right_entities_to_join_paths: Dict[EntityReference, List[EntityIdentifierJoin]] = {}
        for join_path in join_paths_to_visit_next:
            assert len(join_path) > 0
            right_entities_to_join_paths[join_path[-1].right_entity_reference] = join_path

        self._get_remaining_hops_of_joinable_entities(
            left_entity_reference=left_entity_reference,
            parent_entity_to_join_paths=right_entities_to_join_paths,
            known_entity_joins=known_entity_joins,
            join_hops_remaining=join_hops_remaining,
        )

    def get_valid_entity_identifier_join_type(
        self,
        left_entity_reference: EntityReference,
        right_entity_reference: EntityReference,
        on_identifier_reference: IdentifierReference,
    ) -> Optional[EntityIdentifierJoinType]:
        """Get valid join type used to join entities on given identifier, if exists."""
        left_identifier = self._entity_semantics.get_identifier_in_entity(
            EntityElementReference.create_from_references(left_entity_reference, on_identifier_reference)
        )

        right_identifier = self._entity_semantics.get_identifier_in_entity(
            EntityElementReference.create_from_references(right_entity_reference, on_identifier_reference)
        )
        if left_identifier is None or right_identifier is None:
            return None

        left_entity = self._entity_semantics.get_by_reference(left_entity_reference)
        right_entity = self._entity_semantics.get_by_reference(right_entity_reference)
        assert left_entity, "Type refinement. If you see this error something has refactored wrongly"
        assert right_entity, "Type refinement. If you see this error something has refactored wrongly"

        if left_entity.has_validity_dimensions and right_entity.has_validity_dimensions:
            # We cannot join two entities with validity dimensions due to concerns with unexpected fanout
            # due to the key structure of these entities. Applying multi-stage validity window filters can
            # also lead to unexpected removal of interim join keys. Note this will need to be updated if we enable
            # measures in such entities, since those will need to be converted to a different type of entity
            # to support measure computation.
            return None

        if right_identifier.type is IdentifierType.NATURAL:
            if not right_entity.has_validity_dimensions:
                # There is no way to refine this to a single row per key, so we cannot support this join
                return None

        join_type = EntityIdentifierJoinType(left_identifier.type, right_identifier.type)

        if join_type in EntityJoinEvaluator._VALID_IDENTIFIER_JOINS:
            return join_type
        elif join_type in EntityJoinEvaluator._INVALID_IDENTIFIER_JOINS:
            return None

        raise RuntimeError(f"Join type not handled: {join_type}")

    def is_valid_entity_join(
        self,
        left_entity_reference: EntityReference,
        right_entity_reference: EntityReference,
        on_identifier_reference: IdentifierReference,
    ) -> bool:
        """Return true if we should allow a join with the given parameters to resolve a query."""
        return (
            self.get_valid_entity_identifier_join_type(
                left_entity_reference=left_entity_reference,
                right_entity_reference=right_entity_reference,
                on_identifier_reference=on_identifier_reference,
            )
            is not None
        )

    @staticmethod
    def _entity_of_identifier_in_instance_set(
        instance_set: InstanceSet,
        identifier_reference: IdentifierReference,
    ) -> EntityReference:
        """Return the entity where the identifier was defined in the instance set."""
        matching_instances: List[IdentifierInstance] = []
        for identifier_instance in instance_set.identifier_instances:
            assert len(identifier_instance.defined_from) == 1
            if (
                len(identifier_instance.spec.identifier_links) == 0
                and identifier_instance.spec.reference == identifier_reference
            ):
                matching_instances.append(identifier_instance)

        assert len(matching_instances) == 1, (
            f"Not exactly 1 matching identifier instances found: {matching_instances} for {identifier_reference} in "
            f"{pformat_big_objects(instance_set)}"
        )
        return matching_instances[0].origin_entity_reference.entity_reference

    def is_valid_instance_set_join(
        self,
        left_instance_set: InstanceSet,
        right_instance_set: InstanceSet,
        on_identifier_reference: IdentifierReference,
    ) -> bool:
        """Return true if the instance sets can be joined using the given identifier."""
        return self.is_valid_entity_join(
            left_entity_reference=EntityJoinEvaluator._entity_of_identifier_in_instance_set(
                instance_set=left_instance_set, identifier_reference=on_identifier_reference
            ),
            right_entity_reference=EntityJoinEvaluator._entity_of_identifier_in_instance_set(
                instance_set=right_instance_set,
                identifier_reference=on_identifier_reference,
            ),
            on_identifier_reference=on_identifier_reference,
        )
