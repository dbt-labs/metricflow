from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Mapping
from functools import cached_property
from typing import DefaultDict, Iterable, Mapping

from dbt_semantic_interfaces.type_enums import EntityType

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.dsi.model_object_lookup import ModelObjectLookup
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class EntityJoinType:
    left_entity_type: EntityType
    right_entity_type: EntityType


@fast_frozen_dataclass()
class JoinModelOnRightDescriptor:
    entity_name: str
    right_model_id: SemanticModelId
    join_type: EntityJoinType


class SemanticModelJoinLookup:
    """Lookup for valid joins between semantic models.

    This is a simplified / refactored version of the `SemanticModelJoinEvaluator`.
    """

    _VALID_ENTITY_JOINS: FrozenOrderedSet[EntityJoinType] = FrozenOrderedSet(
        (
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
        )
    )

    _INVALID_ENTITY_JOINS: FrozenOrderedSet[EntityJoinType] = FrozenOrderedSet(
        (
            EntityJoinType(left_entity_type=EntityType.PRIMARY, right_entity_type=EntityType.FOREIGN),
            EntityJoinType(left_entity_type=EntityType.UNIQUE, right_entity_type=EntityType.FOREIGN),
            EntityJoinType(left_entity_type=EntityType.FOREIGN, right_entity_type=EntityType.FOREIGN),
            EntityJoinType(left_entity_type=EntityType.NATURAL, right_entity_type=EntityType.FOREIGN),
            # Natural -> Natural joins are not allowed due to hidden fanout or missing value concerns with
            # multiple validity windows in play
            EntityJoinType(left_entity_type=EntityType.NATURAL, right_entity_type=EntityType.NATURAL),
        )
    )

    def __init__(self, model_lookups: Iterable[ModelObjectLookup]) -> None:  # noqa: D107
        self._model_id_to_lookup: Mapping[SemanticModelId, ModelObjectLookup] = {
            lookup.model_id: lookup for lookup in model_lookups
        }

    @classmethod
    def valid_join_to_entity_types(cls) -> FrozenOrderedSet[EntityType]:
        return FrozenOrderedSet(
            join_type.right_entity_type for join_type in SemanticModelJoinLookup._VALID_ENTITY_JOINS
        )

    @cached_property
    def _entity_name_to_model_lookups(self) -> Mapping[str, OrderedSet[ModelObjectLookup]]:
        entity_name_to_model_ids: dict[str, MutableOrderedSet[SemanticModelId]] = {}
        for model_id, lookup in self._model_id_to_lookup.items():
            for entity in lookup.semantic_model.entities:
                entity_name_to_model_ids[entity.name].add(lookup.model_id)
        return {
            entity_name: FrozenOrderedSet(self._model_id_to_lookup[model_id] for model_id in model_ids)
            for entity_name, model_ids in entity_name_to_model_ids.items()
        }

    @cached_property
    def _entity_name_to_model_ids(self) -> Mapping[str, OrderedSet[SemanticModelId]]:
        entity_name_to_model_ids: DefaultDict[str, MutableOrderedSet[SemanticModelId]] = defaultdict(MutableOrderedSet)
        for model_id, lookup in self._model_id_to_lookup.items():
            for entity in lookup.semantic_model.entities:
                entity_name_to_model_ids[entity.name].add(lookup.model_id)
        return entity_name_to_model_ids

    @cached_property
    def _model_id_to_has_validity_dimensions(self) -> Mapping[SemanticModelId, bool]:
        return {
            model_id: lookup.semantic_model.has_validity_dimensions
            for model_id, lookup in self._model_id_to_lookup.items()
        }

    def get_join_model_on_right_descriptors(
        self, left_model_id: SemanticModelId
    ) -> Mapping[SemanticModelId, OrderedSet[JoinModelOnRightDescriptor]]:
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
                    # measures in such semantic models, since those will need to be converted to a different type of semantic model
                    # to support measure computation.
                    continue

                if right_entity_type is EntityType.NATURAL and not right_model_has_validity_dimension:
                    # There is no way to refine this to a single row per key, so we cannot support this join
                    continue

                right_model_id_to_join_descriptors[right_model_id].add(
                    JoinModelOnRightDescriptor(
                        entity_name=left_entity_name,
                        right_model_id=right_model_id,
                        join_type=join_type,
                    )
                )
        return right_model_id_to_join_descriptors
