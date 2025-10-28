from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Mapping, Set
from functools import cached_property

from dbt_semantic_interfaces.type_enums import EntityType
from typing_extensions import override

from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class EntityJoinType:
    """Describe a type of join between semantic models where entities are of the listed types."""

    left_entity_type: EntityType
    right_entity_type: EntityType


@fast_frozen_dataclass()
class JoinModelOnRightDescriptor:
    """How to join one semantic model onto another, using a specific entity and join type."""

    entity_name: str
    join_type: EntityJoinType


class SemanticModelJoinLookup:
    """Lookup for valid joins between semantic models.

    * This is a simplified / refactored version of the `SemanticModelJoinEvaluator` with no significant logic changes.
    * Migrating `SemanticModelJoinEvaluator` to this one will be done after the semantic graph is fully in place to
      ensure that we have a reference path to compare results in case of differences during rollout.
    """

    _VALID_ENTITY_JOINS: Set[EntityJoinType] = {
        EntityJoinType(left_entity_type=EntityType.PRIMARY, right_entity_type=EntityType.NATURAL),
        EntityJoinType(left_entity_type=EntityType.PRIMARY, right_entity_type=EntityType.PRIMARY),
        EntityJoinType(left_entity_type=EntityType.PRIMARY, right_entity_type=EntityType.UNIQUE),
        EntityJoinType(left_entity_type=EntityType.UNIQUE, right_entity_type=EntityType.NATURAL),
        EntityJoinType(left_entity_type=EntityType.UNIQUE, right_entity_type=EntityType.PRIMARY),
        EntityJoinType(left_entity_type=EntityType.UNIQUE, right_entity_type=EntityType.UNIQUE),
        EntityJoinType(left_entity_type=EntityType.FOREIGN, right_entity_type=EntityType.NATURAL),
        EntityJoinType(left_entity_type=EntityType.FOREIGN, right_entity_type=EntityType.PRIMARY),
        EntityJoinType(left_entity_type=EntityType.FOREIGN, right_entity_type=EntityType.UNIQUE),
        EntityJoinType(left_entity_type=EntityType.NATURAL, right_entity_type=EntityType.PRIMARY),
        EntityJoinType(left_entity_type=EntityType.NATURAL, right_entity_type=EntityType.UNIQUE),
    }

    _INVALID_ENTITY_JOINS: Set[EntityJoinType] = {
        EntityJoinType(left_entity_type=EntityType.PRIMARY, right_entity_type=EntityType.FOREIGN),
        EntityJoinType(left_entity_type=EntityType.UNIQUE, right_entity_type=EntityType.FOREIGN),
        EntityJoinType(left_entity_type=EntityType.FOREIGN, right_entity_type=EntityType.FOREIGN),
        EntityJoinType(left_entity_type=EntityType.NATURAL, right_entity_type=EntityType.FOREIGN),
        # Natural -> Natural joins are not allowed due to hidden fanout or missing value concerns with
        # multiple validity windows in play
        EntityJoinType(left_entity_type=EntityType.NATURAL, right_entity_type=EntityType.NATURAL),
    }

    @override
    def __init__(self, manifest_object_lookup: ManifestObjectLookup) -> None:
        self._model_id_to_lookup = manifest_object_lookup.model_id_to_lookup
        self._entity_name_to_model_lookups = manifest_object_lookup.entity_name_to_model_lookups
        self._entity_name_to_model_ids = manifest_object_lookup.entity_name_to_model_ids

    @cached_property
    def valid_join_to_entity_types(self) -> OrderedSet[EntityType]:
        """Returns the valid entity types that can be on the right side of a join."""
        return FrozenOrderedSet(
            join_type.right_entity_type for join_type in SemanticModelJoinLookup._VALID_ENTITY_JOINS
        )

    @cached_property
    def _model_id_to_has_validity_dimensions(self) -> Mapping[SemanticModelId, bool]:
        return {
            model_id: lookup.semantic_model.has_validity_dimensions
            for model_id, lookup in self._model_id_to_lookup.items()
        }

    def get_join_model_on_right_descriptors(
        self, left_model_id: SemanticModelId
    ) -> Mapping[SemanticModelId, OrderedSet[JoinModelOnRightDescriptor]]:
        """Return descriptors for semantic models that can be joined to `left_model_id`.

        The returned dict maps the ID of the semantic model on the right to the entities in the corresponding model
        that can be used as a join key.
        """
        right_model_id_to_join_descriptors: dict[
            SemanticModelId, MutableOrderedSet[JoinModelOnRightDescriptor]
        ] = defaultdict(MutableOrderedSet)
        left_model_lookup = self._model_id_to_lookup[left_model_id]
        left_model = left_model_lookup.semantic_model
        left_model_has_validity_dimensions = self._model_id_to_has_validity_dimensions[left_model_id]
        for entity in left_model_lookup.semantic_model.entities:
            left_entity_name = entity.name
            left_entity_type = entity.type
            other_model_lookups_with_same_entity_name = self._entity_name_to_model_ids[left_entity_name]
            for right_model_id in other_model_lookups_with_same_entity_name:
                right_model_lookup = self._model_id_to_lookup[right_model_id]
                right_model = right_model_lookup.semantic_model
                right_entity_type = right_model_lookup.entity_lookup.entity_name_to_type[left_entity_name]
                right_model_has_validity_dimension = self._model_id_to_has_validity_dimensions[right_model_id]

                join_type = EntityJoinType(
                    left_entity_type=left_entity_type,
                    right_entity_type=right_entity_type,
                )

                if join_type in SemanticModelJoinLookup._VALID_ENTITY_JOINS:
                    pass
                elif join_type in SemanticModelJoinLookup._INVALID_ENTITY_JOINS:
                    continue
                else:
                    raise ValueError(
                        LazyFormat(
                            "Unknown join type.",
                            join_type=join_type,
                            left_entity_name=left_entity_name,
                            left_model=left_model,
                            right_model=right_model,
                        )
                    )

                if left_model_has_validity_dimensions and right_model_has_validity_dimension:
                    # We cannot join two semantic models with validity dimensions due to concerns with unexpected fanout
                    # due to the key structure of these semantic models. Applying multi-stage validity window filters can
                    # also lead to unexpected removal of interim join keys. Note this will need to be updated if we enable
                    # simple-metric inputs in such semantic models, since those will need to be converted to a different type of semantic model
                    # to support such computation.
                    continue

                if right_entity_type is EntityType.NATURAL and not right_model_has_validity_dimension:
                    # There is no way to refine this to a single row per key, so we cannot support this join
                    continue

                right_model_id_to_join_descriptors[right_model_id].add(
                    JoinModelOnRightDescriptor(
                        entity_name=left_entity_name,
                        join_type=join_type,
                    )
                )
        return right_model_id_to_join_descriptors
