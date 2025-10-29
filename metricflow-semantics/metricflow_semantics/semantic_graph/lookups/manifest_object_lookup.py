from __future__ import annotations

import logging
from collections import defaultdict
from enum import Enum
from functools import cached_property
from typing import Iterable, Mapping, Optional, Sequence

from dbt_semantic_interfaces.protocols import Metric, SemanticManifest, SemanticModel
from dbt_semantic_interfaces.type_enums import MetricType, TimeGranularity
from more_itertools import peekable
from typing_extensions import override

from metricflow_semantics.errors.error_classes import MetricFlowInternalError
from metricflow_semantics.model.semantics.simple_metric_input import SimpleMetricInput
from metricflow_semantics.semantic_graph.lookups.model_object_lookup import (
    ModelObjectLookup,
)
from metricflow_semantics.semantic_graph.lookups.simple_metric_model_object_lookup import SimpleMetricModelObjectLookup
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.time.granularity import ExpandedTimeGranularity
from metricflow_semantics.time.time_spine_source import TimeSpineSource
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.toolkit.mf_logging.attribute_pretty_format import AttributeMapping, AttributePrettyFormattable
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple, Pair, T
from metricflow_semantics.toolkit.syntactic_sugar import mf_first_item, mf_flatten

logger = logging.getLogger(__name__)


class ManifestObjectLookup(AttributePrettyFormattable):
    """Helps retrieve semantic-manifest objects (i.e. ones from `dbt-semantic-interfaces`).

    These streamlined / minimal lookups were added to use for initializing the semantic graph as the current lookup
    classes have significant initialization times relative to initialization time of the semantic-graph-based resolver.

    Using an example high-complexity semantic manifest, initialization of the current lookups took ~0.2s.
    Initialization of the semantic-graph resolver with the same manifest was < 1s, so using the current lookups would
    have resulted in a significant relative increase.

    Initialization of this lookup and an example call is 100x faster (see `test_model_lookup_performance`).

    Future work includes further refinement of these classes and replacing existing `*Lookup` classes with the ones in
    this module / other implementations.
    """

    def __init__(self, semantic_manifest: SemanticManifest) -> None:  # noqa: D107
        self._semantic_manifest = semantic_manifest
        self._time_spine_sources = TimeSpineSource.build_standard_time_spine_sources(semantic_manifest)
        self._custom_grains = TimeSpineSource.build_custom_granularities(self._time_spine_sources.values())

    @cached_property
    def semantic_manifest(self) -> SemanticManifest:  # noqa: D102
        return self._semantic_manifest

    @cached_property
    def semantic_models(self) -> Sequence[SemanticModel]:  # noqa: D102
        return self._semantic_manifest.semantic_models

    @cached_property
    def _semantic_model_and_simple_metrics_pairs(self) -> Sequence[Pair[SemanticModel, Sequence[Metric]]]:
        model_name_to_simple_metrics: defaultdict[str, list[Metric]] = defaultdict(list)

        for metric in self._semantic_manifest.metrics:
            metric_type = metric.type
            if metric_type is MetricType.SIMPLE:
                metric_aggregation_params = metric.type_params.metric_aggregation_params
                if metric_aggregation_params is None:
                    raise MetricFlowInternalError(
                        LazyFormat("A simple metric is missing `metric_aggregation_params`", metric=metric)
                    )
                model_name_to_simple_metrics[metric_aggregation_params.semantic_model].append(metric)

        semantic_model_and_simple_metrics_pairs: list[Pair[SemanticModel, Sequence[Metric]]] = []
        for semantic_model in self._semantic_manifest.semantic_models:
            semantic_model_and_simple_metrics_pairs.append(
                (semantic_model, model_name_to_simple_metrics.get(semantic_model.name) or ())
            )
        return semantic_model_and_simple_metrics_pairs

    @cached_property
    def simple_metric_model_lookups(self) -> AnyLengthTuple[SimpleMetricModelObjectLookup]:
        """Returns lookups corresponding to semantic models that are associated with simple metrics."""
        return tuple(
            SimpleMetricModelObjectLookup(semantic_model, simple_metrics=simple_metrics)
            for semantic_model, simple_metrics in self._semantic_model_and_simple_metrics_pairs
            if len(simple_metrics) > 0
        )

    @cached_property
    def simple_metric_exclusive_model_lookups(self) -> AnyLengthTuple[ModelObjectLookup]:
        """Returns lookups corresponding to semantic models that are not associated with simple metrics."""
        return tuple(
            ModelObjectLookup(semantic_model)
            for semantic_model, simple_metrics in self._semantic_model_and_simple_metrics_pairs
            if len(simple_metrics) == 0
        )

    @cached_property
    def model_object_lookups(self) -> AnyLengthTuple[ModelObjectLookup]:
        """Return lookups for all semantic models."""
        return self.simple_metric_model_lookups + self.simple_metric_exclusive_model_lookups

    @cached_property
    def model_id_to_lookup(self) -> Mapping[SemanticModelId, ModelObjectLookup]:  # noqa: D102
        return {lookup.model_id: lookup for lookup in self.model_object_lookups}

    @cached_property
    def model_id_to_simple_metric_model_lookup(  # noqa: D102
        self,
    ) -> Mapping[SemanticModelId, SimpleMetricModelObjectLookup]:
        return {lookup.model_id: lookup for lookup in self.simple_metric_model_lookups}

    @cached_property
    def simple_metric_name_to_input(self) -> Mapping[str, SimpleMetricInput]:  # noqa: D102
        return {
            simple_metric_input.name: simple_metric_input
            for lookup in self.simple_metric_model_lookups
            for simple_metric_inputs in lookup.aggregation_configuration_to_simple_metric_inputs.values()
            for simple_metric_input in simple_metric_inputs
        }

    @cached_property
    def entity_name_to_model_lookups(self) -> Mapping[str, OrderedSet[ModelObjectLookup]]:
        """Mapping from the entity name to the model lookups that have the entity."""
        entity_name_to_model_lookups: dict[str, MutableOrderedSet[ModelObjectLookup]] = defaultdict(MutableOrderedSet)
        for model_id, lookup in self.model_id_to_lookup.items():
            for entity in lookup.semantic_model.entities:
                entity_name_to_model_lookups[entity.name].add(lookup)
        return entity_name_to_model_lookups

    @cached_property
    def entity_name_to_model_ids(self) -> Mapping[str, OrderedSet[SemanticModelId]]:
        """Mapping from the entity name to the IDs of the semantic models that contain it."""
        return {
            entity_name: FrozenOrderedSet(model_lookup.model_id for model_lookup in model_lookups)
            for entity_name, model_lookups in self.entity_name_to_model_lookups.items()
        }

    def get_metric(self, metric_name: str) -> Metric:  # noqa: D102
        return self._lookup_object(
            value_type=_ValueType.METRIC,
            name=metric_name,
            name_to_object_mapping=self._metric_name_to_metric,
        )

    def get_metrics(self) -> Iterable[Metric]:  # noqa: D102
        return self._metric_name_to_metric.values()

    @cached_property
    def min_time_grain_in_time_spine(self) -> TimeGranularity:
        """Return the smallest time grain as configured in the time spine."""
        return mf_first_item(sorted(self._time_spine_sources.keys()))

    @cached_property
    def min_time_grain_used_in_models(self) -> Optional[TimeGranularity]:
        """Return the smallest time grain that's used to define a time dimension."""
        time_grains = mf_flatten(
            model_object_lookup.time_dimension_name_to_grain.values()
            for model_object_lookup in self.model_object_lookups
        )
        peekable_grains = peekable(time_grains)
        try:
            peekable_grains.peek()
        except StopIteration:
            return None

        return min(peekable_grains)

    @cached_property
    def expanded_time_grains(self) -> AnyLengthTuple[ExpandedTimeGranularity]:
        """Return the expanded time grains as configured in the time spine."""
        return tuple(self._custom_grains.values())

    def _lookup_object(self, value_type: _ValueType, name: str, name_to_object_mapping: Mapping[str, T]) -> T:
        """Helper method to look up an object in a mapping and raise a helpful error if it is not found."""
        try:
            return name_to_object_mapping[name]
        except KeyError as e:
            raise KeyError(
                LazyFormat(
                    "An object with the given name is not known",
                    value_type=value_type,
                    name=name,
                    known_names=list(name_to_object_mapping.keys()),
                )
            ) from e

    @cached_property
    def _metric_name_to_metric(self) -> Mapping[str, Metric]:
        metric_name_to_metric: dict[str, Metric] = {}

        for metric in self._semantic_manifest.metrics:
            metric_name_to_metric[metric.name] = metric
        return metric_name_to_metric

    @cached_property
    @override
    def _attribute_mapping(self) -> AttributeMapping:
        return dict(
            **super()._attribute_mapping,
            **{
                attribute_name: getattr(self, attribute_name)
                for attribute_name in (
                    "model_object_lookups",
                    "min_time_grain_in_time_spine",
                    "min_time_grain_used_in_models",
                    "expanded_time_grains",
                )
            },
        )


class _ValueType(Enum):
    """Different types of objects stored as values in lookup dictionaries."""

    ENTITY = "entity"
    METRIC = "metric"
    MODEL_ID = "model_id"
