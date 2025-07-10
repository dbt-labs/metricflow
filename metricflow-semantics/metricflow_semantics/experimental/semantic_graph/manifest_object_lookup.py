from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from functools import cached_property
from typing import Iterable, Mapping, TypeVar

from dbt_semantic_interfaces.protocols import Metric, SemanticManifest, SemanticModel
from dbt_semantic_interfaces.type_enums import DimensionType, TimeGranularity

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.metricflow_exception import MetricflowAssertionError
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.model_object_lookup import (
    MeasureContainingModelObjectLookup,
    SemanticModelObjectLookup,
)
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.time.granularity import ExpandedTimeGranularity
from metricflow_semantics.time.time_spine_source import TimeSpineSource

ObjectT = TypeVar("ObjectT")


class ManifestObjectLookup:
    """Helps retrieve semantic manifest objects (i.e. ones from `dbt-semantic-interfaces`).

    Cached mappings are generated as needed to reduce initialization times. Note that generating the current lookups
    are simple, so this may not make a significant difference.
    """

    def __init__(self, semantic_manifest: SemanticManifest) -> None:  # noqa: D107
        self._semantic_manifest = semantic_manifest

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
            model_id = SemanticModelId(model_name=semantic_model.name)
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
            object_type="entity",
            name=entity_name,
            name_to_object_mapping=self._entity_name_to_semantic_model,
        )

    def get_semantic_model_by_name(self, semantic_model_name: str) -> SemanticModel:  # noqa: D102
        return self._lookup_object(
            object_type="semantic_model",
            name=semantic_model_name,
            name_to_object_mapping=self._semantic_model_name_to_semantic_model,
        )

    def get_semantic_model_by_id(self, semantic_model_id: SemanticModelId) -> SemanticModel:  # noqa: D102
        return self._lookup_object(
            object_type="semantic_model",
            name=semantic_model_id.model_name,
            name_to_object_mapping=self._semantic_model_name_to_semantic_model,
        )

    def get_metric(self, metric_name: str) -> Metric:  # noqa: D102
        return self._lookup_object(
            object_type="metric",
            name=metric_name,
            name_to_object_mapping=self._metric_name_to_metric,
        )

    def get_metrics(self) -> Iterable[Metric]:
        return self._metric_name_to_metric.values()

    def get_semantic_model_containing_measure(self, measure_name: str) -> SemanticModel:  # noqa: D102
        return self._lookup_object(
            object_type="measure",
            name=measure_name,
            name_to_object_mapping=self._measure_name_to_semantic_model,
        )

    @cached_property
    def min_time_grain(self) -> TimeGranularity:
        """Return the smallest time grain that's used in a definition of a dimension."""
        time_grains: set[TimeGranularity] = set()
        for semantic_model in self._semantic_manifest.semantic_models:
            for dimension in semantic_model.dimensions:
                if dimension.type is DimensionType.TIME:
                    assert dimension.type_params is not None
                    time_grains.add(dimension.type_params.time_granularity)
                elif dimension.type is DimensionType.CATEGORICAL:
                    pass
        sorted_time_grains = sorted(time_grains, key=lambda time_grain: time_grain.to_int())
        if len(sorted_time_grains) == 0:
            raise ValueError("No valid time grains were found for any dimension.")

        return sorted_time_grains[0]

    @cached_property
    def expanded_time_grains(self) -> AnyLengthTuple[ExpandedTimeGranularity]:
        time_spine_sources = TimeSpineSource.build_standard_time_spine_sources(self._semantic_manifest)

        return tuple(TimeSpineSource.build_custom_granularities(time_spine_sources.values()).values())

    def _lookup_object(self, object_type: str, name: str, name_to_object_mapping: Mapping[str, ObjectT]) -> ObjectT:
        try:
            return name_to_object_mapping[name]
        except KeyError as e:
            raise MetricflowAssertionError(
                LazyFormat(
                    "An object with the given name is not known",
                    type=object_type,
                    name=name,
                    known_names=FrozenOrderedSet(name_to_object_mapping.keys()),
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
                    raise MetricflowAssertionError(
                        LazyFormat(
                            "Measure was found in multiple semantic models. This should have been caught in validation.",
                            measure=measure.name,
                            semantic_model=self._measure_name_to_semantic_model[measure.name].name,
                            other_semantic_model=semantic_model.name,
                        )
                    )
                measure_name_to_semantic_model[measure.name] = semantic_model
        return measure_name_to_semantic_model
