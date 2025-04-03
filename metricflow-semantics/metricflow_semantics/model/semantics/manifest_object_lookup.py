from __future__ import annotations

from collections import defaultdict
from typing import Dict, List

from dbt_semantic_interfaces.protocols import Metric, SemanticManifest, SemanticModel
from dbt_semantic_interfaces.references import (
    EntityReference,
    MeasureReference,
    MetricReference,
    SemanticModelReference,
)

from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat


class SemanticManifestObjectLookup:
    """Helps retrieve semantic manifest objects (i.e. ones defined in `dbt-semantic-interfaces`).

    There are similarities to the other `*Lookup` classes like `SemanticModelLookup`. Some consolidation is needed.
    """

    def __init__(self, semantic_manifest: SemanticManifest) -> None:  # noqa: D107
        self._metric_references_to_metrics: Dict[MetricReference, Metric] = {}

        for metric in semantic_manifest.metrics:
            self._metric_references_to_metrics[MetricReference(metric.name)] = metric

        self._entity_to_semantic_model: Dict[EntityReference, List[SemanticModel]] = defaultdict(list)
        self._semantic_model_reference_to_semantic_model: Dict[SemanticModelReference, SemanticModel] = {}
        self._measure_to_semantic_model: Dict[MeasureReference, SemanticModel] = {}

        for semantic_model in semantic_manifest.semantic_models:
            for entity in semantic_model.entities:
                self._entity_to_semantic_model[entity.reference].append(semantic_model)
            for measure in semantic_model.measures:
                measure_reference = measure.reference
                if measure_reference in self._measure_to_semantic_model:
                    raise RuntimeError(
                        LazyFormat(
                            "Measure was found in multiple semantic models. This should have been caught in validation.",
                            measure_reference=measure_reference,
                            semantic_model=self._measure_to_semantic_model[measure_reference].reference,
                            other_semantic_model=semantic_model.reference,
                        ).evaluated_value
                    )
                self._measure_to_semantic_model[measure_reference] = semantic_model

            self._semantic_model_reference_to_semantic_model[semantic_model.reference] = semantic_model

        for metric in semantic_manifest.metrics:
            self._metric_references_to_metrics[MetricReference(metric.name)] = metric

    def get_semantic_models_containing_entity(  # noqa: D102
        self, entity_reference: EntityReference
    ) -> List[SemanticModel]:
        return self._entity_to_semantic_model[entity_reference]

    def get_semantic_model_by_reference(self, reference: SemanticModelReference) -> SemanticModel:  # noqa: D102
        return self._semantic_model_reference_to_semantic_model[reference]

    def get_metric_by_reference(self, reference: MetricReference) -> Metric:  # noqa: D102
        return self._metric_references_to_metrics[reference]

    def get_semantic_model_containing_measure(self, measure_reference: MeasureReference) -> SemanticModel:  # noqa: D102
        return self._measure_to_semantic_model[measure_reference]
