from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Sequence
from enum import Enum
from functools import cached_property
from typing import DefaultDict, Iterable, Mapping

from dbt_semantic_interfaces.protocols import Metric, SemanticManifest, SemanticModel
from dbt_semantic_interfaces.type_enums import EntityType, TimeGranularity

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple, T
from metricflow_semantics.collection_helpers.syntactic_sugar import mf_first_item
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.dsi.model_object_lookup import (
    MeasureContainingModelObjectLookup,
    SemanticModelObjectLookup,
)
from metricflow_semantics.experimental.metricflow_exception import InvalidManifestException
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.time.granularity import ExpandedTimeGranularity
from metricflow_semantics.time.time_spine_source import TimeSpineSource

logger = logging.getLogger(__name__)


class ManifestObjectLookup:
    """Helps retrieve semantic manifest objects (i.e. ones from `dbt-semantic-interfaces`).

    Cached mappings are generated as needed to reduce initialization times. Note that generating the current lookups
    are simple, so this may not make a significant difference.
    """

    def __init__(self, semantic_manifest: SemanticManifest) -> None:  # noqa: D107
        self._semantic_manifest = semantic_manifest
        self._time_spine_sources = TimeSpineSource.build_standard_time_spine_sources(semantic_manifest)
        self._custom_grains = TimeSpineSource.build_custom_granularities(self._time_spine_sources.values())

    @property
    def semantic_manifest(self) -> SemanticManifest:  # noqa: D102
        return self._semantic_manifest

    @property
    def semantic_models(self) -> Iterable[SemanticModel]:
        return self._semantic_manifest.semantic_models

    @cached_property
    def semantic_model_name_to_lookup(self) -> Mapping[str, SemanticModelObjectLookup]:
        return {
            semantic_model.name: SemanticModelObjectLookup(semantic_model)
            for semantic_model in self._semantic_manifest.semantic_models
        }

    @cached_property
    def measure_containing_model_lookups(self) -> AnyLengthTuple[MeasureContainingModelObjectLookup]:
        return tuple(
            MeasureContainingModelObjectLookup(semantic_model)
            for semantic_model in self.semantic_models
            if len(semantic_model.measures) > 0
        )

    @cached_property
    def measure_name_to_model_id(self) -> Mapping[str, SemanticModelId]:
        return {
            measure.name: SemanticModelId.get_instance(semantic_model.name)
            for semantic_model in self.semantic_models
            for measure in semantic_model.measures
        }

    @cached_property
    def entity_name_to_joinable_semantic_model_id(self) -> Mapping[str, OrderedSet[SemanticModelId]]:
        result: dict[str, MutableOrderedSet[SemanticModelId]] = defaultdict(MutableOrderedSet[SemanticModelId])
        for semantic_model in self.semantic_models:
            model_id = SemanticModelId.get_instance(model_name=semantic_model.name)
            for entity in semantic_model.entities:
                if entity.is_linkable_entity_type:
                    result[entity.name].add(model_id)

        return result

    @cached_property
    def non_measure_containing_model_lookups(self) -> AnyLengthTuple[SemanticModelObjectLookup]:
        return tuple(
            SemanticModelObjectLookup(semantic_model)
            for semantic_model in self.semantic_models
            if len(semantic_model.measures) == 0
        )

    @cached_property
    def model_object_lookups(self) -> AnyLengthTuple[SemanticModelObjectLookup]:
        return self.measure_containing_model_lookups + self.non_measure_containing_model_lookups

    def get_semantic_models_containing_entity(self, entity_name: str) -> Sequence[SemanticModel]:  # noqa: D102
        return self._lookup_object(
            object_type=_ManifestObjectType.ENTITY,
            name=entity_name,
            name_to_object_mapping=self._entity_name_to_semantic_model,
        )

    def get_semantic_model_by_name(self, semantic_model_name: str) -> SemanticModel:  # noqa: D102
        return self._lookup_object(
            object_type=_ManifestObjectType.MODEL,
            name=semantic_model_name,
            name_to_object_mapping=self._semantic_model_name_to_semantic_model,
        )

    def get_semantic_model_by_id(self, semantic_model_id: SemanticModelId) -> SemanticModel:  # noqa: D102
        return self._lookup_object(
            object_type=_ManifestObjectType.MODEL,
            name=semantic_model_id.model_name,
            name_to_object_mapping=self._semantic_model_name_to_semantic_model,
        )

    def get_metric(self, metric_name: str) -> Metric:  # noqa: D102
        return self._lookup_object(
            object_type=_ManifestObjectType.METRIC,
            name=metric_name,
            name_to_object_mapping=self._metric_name_to_metric,
        )

    def get_metrics(self) -> Iterable[Metric]:
        return self._metric_name_to_metric.values()

    def get_semantic_model_containing_measure(self, measure_name: str) -> SemanticModel:  # noqa: D102
        return self._lookup_object(
            object_type=_ManifestObjectType.MEASURE,
            name=measure_name,
            name_to_object_mapping=self._measure_name_to_semantic_model,
        )

    @cached_property
    def min_time_grain(self) -> TimeGranularity:
        return mf_first_item(sorted(self._time_spine_sources.keys()))

    @cached_property
    def expanded_time_grains(self) -> AnyLengthTuple[ExpandedTimeGranularity]:
        return tuple(self._custom_grains.values())

    def _lookup_object(self, object_type: _ManifestObjectType, name: str, name_to_object_mapping: Mapping[str, T]) -> T:
        try:
            return name_to_object_mapping[name]
        except KeyError as e:
            raise KeyError(
                LazyFormat(
                    "An object with the given name is not known",
                    object_type=object_type,
                    name=name,
                    known_names=list(name_to_object_mapping.keys()),
                )
            ) from e

    @cached_property
    def _metric_name_to_metric(self) -> dict[str, Metric]:
        metric_name_to_metric: dict[str, Metric] = {}

        for metric in self._semantic_manifest.metrics:
            metric_name_to_metric[metric.name] = metric
        return metric_name_to_metric

    @cached_property
    def _entity_name_to_semantic_model(self) -> dict[str, list[SemanticModel]]:
        entity_name_to_semantic_model = defaultdict(list)
        for semantic_model in self._semantic_manifest.semantic_models:
            for entity in semantic_model.entities:
                entity_name_to_semantic_model[entity.name].append(semantic_model)
        return entity_name_to_semantic_model

    @cached_property
    def _semantic_model_name_to_semantic_model(self) -> dict[str, SemanticModel]:
        semantic_model_name_to_semantic_model = {}
        for semantic_model in self._semantic_manifest.semantic_models:
            semantic_model_name_to_semantic_model[semantic_model.name] = semantic_model
        return semantic_model_name_to_semantic_model

    @cached_property
    def _measure_name_to_semantic_model(self) -> dict[str, SemanticModel]:
        measure_name_to_semantic_model: dict[str, SemanticModel] = {}
        for semantic_model in self._semantic_manifest.semantic_models:
            for measure in semantic_model.measures:
                if measure.name in measure_name_to_semantic_model:
                    raise InvalidManifestException(
                        LazyFormat(
                            "Measure was found in multiple semantic models. This should have been caught in validation.",
                            measure=measure.name,
                            semantic_model=self._measure_name_to_semantic_model[measure.name].name,
                            other_semantic_model=semantic_model.name,
                        )
                    )
                measure_name_to_semantic_model[measure.name] = semantic_model
        return measure_name_to_semantic_model


class _ManifestObjectType(Enum):
    """Different types of objects in the semantic manifest used to key private lookup dictionaries."""

    ENTITY = "entity"
    MEASURE = "measure"
    METRIC = "metric"
    MODEL = "model"


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

    def __init__(self, model_lookups: Iterable[SemanticModelObjectLookup]) -> None:  # noqa: D107
        self._model_id_to_lookup: Mapping[SemanticModelId, SemanticModelObjectLookup] = {
            lookup.model_id: lookup for lookup in model_lookups
        }

    @classmethod
    def valid_join_to_entity_types(cls) -> FrozenOrderedSet[EntityType]:
        return FrozenOrderedSet(
            join_type.right_entity_type for join_type in SemanticModelJoinLookup._VALID_ENTITY_JOINS
        )

    @cached_property
    def _entity_name_to_model_lookups(self) -> Mapping[str, OrderedSet[SemanticModelObjectLookup]]:
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
