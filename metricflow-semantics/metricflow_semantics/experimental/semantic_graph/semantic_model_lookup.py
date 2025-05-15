from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import List, Mapping, TypeVar

from dbt_semantic_interfaces.protocols import Metric, SemanticManifest, SemanticModel

from metricflow_semantics.experimental.metricflow_exception import MetricflowAssertionError
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

ObjectT = TypeVar("ObjectT")


class SemanticManifestObjectLookup:
    """Helps retrieve semantic manifest objects (i.e. ones defined in `dbt-semantic-interfaces`)."""

    def __init__(self, semantic_manifest: SemanticManifest) -> None:  # noqa: D107
        self._metric_name_to_metric: dict[str, Metric] = {}

        for metric in semantic_manifest.metrics:
            self._metric_name_to_metric[metric.name] = metric

        self._entity_name_to_semantic_model: dict[str, List[SemanticModel]] = defaultdict(list)
        self._semantic_model_name_to_semantic_model: dict[str, SemanticModel] = {}
        self._measure_name_to_semantic_model: dict[str, SemanticModel] = {}

        for semantic_model in semantic_manifest.semantic_models:
            for entity in semantic_model.entities:
                self._entity_name_to_semantic_model[entity.name].append(semantic_model)
            for measure in semantic_model.measures:
                if measure.name in self._measure_name_to_semantic_model:
                    raise MetricflowAssertionError(
                        LazyFormat(
                            "Measure was found in multiple semantic models. This should have been caught in validation.",
                            measure=measure.name,
                            semantic_model=self._measure_name_to_semantic_model[measure.name].name,
                            other_semantic_model=semantic_model.name,
                        )
                    )
                self._measure_name_to_semantic_model[measure.name] = semantic_model

            self._semantic_model_name_to_semantic_model[semantic_model.name] = semantic_model

        for metric in semantic_manifest.metrics:
            self._metric_name_to_metric[metric.name] = metric

    def get_semantic_models_containing_entity(self, entity_name: str) -> Sequence[SemanticModel]:  # noqa: D102
        return self._lookup_object(
            object_type="entity",
            name=entity_name,
            name_to_object_mapping=self._entity_name_to_semantic_model,
        )

    def get_semantic_model(self, semantic_model_name: str) -> SemanticModel:  # noqa: D102
        return self._lookup_object(
            object_type="semantic_model",
            name=semantic_model_name,
            name_to_object_mapping=self._semantic_model_name_to_semantic_model,
        )

    def get_metric(self, metric_name: str) -> Metric:  # noqa: D102
        return self._lookup_object(
            object_type="metric",
            name=metric_name,
            name_to_object_mapping=self._metric_name_to_metric,
        )

    def get_semantic_model_containing_measure(self, measure_name: str) -> SemanticModel:  # noqa: D102
        return self._lookup_object(
            object_type="measure",
            name=measure_name,
            name_to_object_mapping=self._measure_name_to_semantic_model,
        )

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
