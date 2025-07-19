from __future__ import annotations

import logging
from enum import Enum
from functools import cached_property
from typing import Iterable, Mapping

from dbt_semantic_interfaces.protocols import Metric, SemanticManifest, SemanticModel
from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple, T
from metricflow_semantics.collection_helpers.syntactic_sugar import mf_first_item
from metricflow_semantics.experimental.dsi.measure_model_object_lookup import MeasureContainingModelObjectLookup
from metricflow_semantics.experimental.dsi.model_object_lookup import (
    ModelObjectLookup,
)
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
    def semantic_models(self) -> Iterable[SemanticModel]:  # noqa: D102
        return self._semantic_manifest.semantic_models

    @cached_property
    def semantic_model_name_to_lookup(self) -> Mapping[str, ModelObjectLookup]:
        """Mapping from the name of the semantic model to the lookup for objects in that model."""
        return {
            semantic_model.name: ModelObjectLookup(semantic_model)
            for semantic_model in self._semantic_manifest.semantic_models
        }

    @cached_property
    def measure_containing_model_lookups(self) -> AnyLengthTuple[MeasureContainingModelObjectLookup]:
        """Returns lookups corresponding to semantic models that contain measures."""
        return tuple(
            MeasureContainingModelObjectLookup(semantic_model)
            for semantic_model in self.semantic_models
            if len(semantic_model.measures) > 0
        )

    @cached_property
    def measure_exclusive_model_lookups(self) -> AnyLengthTuple[ModelObjectLookup]:
        """Returns lookups corresponding to semantic models that do not contain measures."""
        return tuple(
            ModelObjectLookup(semantic_model)
            for semantic_model in self.semantic_models
            if len(semantic_model.measures) == 0
        )

    @cached_property
    def model_object_lookups(self) -> AnyLengthTuple[ModelObjectLookup]:
        """Return lookups for all semantic models."""
        return self.measure_containing_model_lookups + self.measure_exclusive_model_lookups

    def get_metric(self, metric_name: str) -> Metric:  # noqa: D102
        return self._lookup_object(
            object_type=_LookupMappingValueType.METRIC,
            name=metric_name,
            name_to_object_mapping=self._metric_name_to_metric,
        )

    def get_metrics(self) -> Iterable[Metric]:  # noqa: D102
        return self._metric_name_to_metric.values()

    def get_model_id_for_measure(self, measure_name: str) -> SemanticModelId:  # noqa: D102
        return self._lookup_object(
            object_type=_LookupMappingValueType.MODEL_ID,
            name=measure_name,
            name_to_object_mapping=self._measure_name_to_model_id,
        )

    @cached_property
    def min_time_grain(self) -> TimeGranularity:
        """Return the smallest time grain as configured in the time spine."""
        return mf_first_item(sorted(self._time_spine_sources.keys()))

    @cached_property
    def expanded_time_grains(self) -> AnyLengthTuple[ExpandedTimeGranularity]:
        """Return the expanded time grains as configured in the time spine."""
        return tuple(self._custom_grains.values())

    def _lookup_object(
        self, object_type: _LookupMappingValueType, name: str, name_to_object_mapping: Mapping[str, T]
    ) -> T:
        """Helper method to look up an object in a mapping and raise a helpful error if it is not found."""
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
    def _measure_name_to_model_id(self) -> Mapping[str, SemanticModelId]:
        """Mapping from the name of the measure to the associated semantic-model ID."""
        return {
            measure.name: SemanticModelId.get_instance(semantic_model.name)
            for semantic_model in self.semantic_models
            for measure in semantic_model.measures
        }

    @cached_property
    def _metric_name_to_metric(self) -> dict[str, Metric]:
        metric_name_to_metric: dict[str, Metric] = {}

        for metric in self._semantic_manifest.metrics:
            metric_name_to_metric[metric.name] = metric
        return metric_name_to_metric


class _LookupMappingValueType(Enum):
    """Different types of objects in the semantic manifest used to key private lookup dictionaries."""

    ENTITY = "entity"
    MEASURE = "measure"
    METRIC = "metric"
    MODEL = "model"
    MODEL_ID = "model_id"
