# from __future__ import annotations
#
# from collections import defaultdict
# from typing import Dict, Generic, List, Sequence, Set, Union
#
# from dbt_semantic_interfaces.protocols import (
#     Metric,
#     SavedQuery,
#     SemanticManifest,
#     SemanticManifestT,
#     SemanticModel,
# )
# from dbt_semantic_interfaces.references import (
#     DimensionReference,
#     EntityReference,
#     LinkableElementReference,
#     MeasureReference,
#     MetricReference,
#     SavedQueryReference,
#     SemanticModelReference,
#     TimeDimensionReference,
# )
#
# GroupByElementReference = Union[DimensionReference, TimeDimensionReference, EntityReference]
#
#
# class SemanticModelLookup:
#     def __init__(self, semantic_manifest: SemanticManifest) -> None:  # noqa: D
#         model_by_measure: Dict[MeasureReference, Set[SemanticModelReference]] = defaultdict(set)
#         model_by_linkable_element: Dict[LinkableElementReference, Set[SemanticModelReference]] = defaultdict(set)
#
#         semantic_model: SemanticModel
#         self._semantic_models: List[SemanticModel] = []
#
#         for semantic_model in semantic_manifest.semantic_models:
#             self._semantic_models.append(semantic_model)
#
#             for measure_reference in semantic_model.measure_references:
#                 model_by_measure[measure_reference].add(semantic_model.reference)
#             for entity_reference in semantic_model.entity_references:
#                 model_by_linkable_element[entity_reference].add(semantic_model.reference)
#             for dimension_reference in semantic_model.dimension_references:
#                 model_by_linkable_element[dimension_reference].add(semantic_model.reference)
#
#         self._models_by_measure = {
#             measure_reference: tuple(sorted(model_references))
#             for measure_reference, model_references in model_by_measure.items()
#         }
#         self._models_by_linkable_element = {
#             linkable_element_reference: tuple(sorted(model_references))
#             for linkable_element_reference, model_references in model_by_linkable_element.items()
#         }
#         self._model_by_reference = {
#             semantic_model.reference: semantic_model for semantic_model in semantic_manifest.semantic_models
#         }
#
#     def _get_models(self, model_references: Sequence[SemanticModelReference]) -> Sequence[SemanticModel]:
#         return tuple(self._model_by_reference[model_reference] for model_reference in model_references)
#
#     def get_models_containing_measure(self, measure_reference: MeasureReference) -> Sequence[SemanticModel]:
#         return self._get_models(self._models_by_measure[measure_reference])
#
#     def get_models_containing_entity(self, entity_reference: EntityReference) -> Sequence[SemanticModel]:
#         return self._get_models(self._models_by_linkable_element[entity_reference])
#
#     def get_models_containing_dimension(self, dimension_reference: DimensionReference) -> Sequence[SemanticModel]:
#         return self._get_models(self._models_by_linkable_element[dimension_reference])
#
#     @property
#     def models(self) -> Sequence[SemanticModel]:
#         return self._semantic_models
#
#
# class MetricDependencyLookup(Generic[SemanticManifestT]):
#     def __init__(self, semantic_manifest: SemanticManifestT) -> None:  # noqa: D
#         parent_metric_index: Dict[MetricReference, Set[MetricReference]] = defaultdict(set)
#         parent_measure_index: Dict[MetricReference, Set[MeasureReference]] = defaultdict(set)
#         for metric in semantic_manifest.metrics:
#             for input_metric in metric.input_metrics:
#                 parent_metric_index[metric.reference].add(input_metric.as_reference)
#
#             for input_measure in metric.input_measures:
#                 parent_measure_index[metric.reference].add(input_measure.measure_reference)
#
#         self._parent_metric_index = {
#             metric_reference: tuple(sorted(parent_metric_references))
#             for metric_reference, parent_metric_references in parent_metric_index.items()
#         }
#         self._parent_measure_index = {
#             metric_reference: tuple(sorted(parent_metric_references))
#             for metric_reference, parent_metric_references in parent_measure_index.items()
#         }
#
#     def get_parent_metrics(self, metric_reference: MetricReference) -> Sequence[MetricReference]:
#         return self._parent_metric_index.get(metric_reference) or ()
#
#     def get_parent_measures(self, metric_reference: MetricReference) -> Sequence[MeasureReference]:
#         return self._parent_measure_index.get(metric_reference) or ()
#
#
# class SavedQueryLookup:
#     def __init__(self, semantic_manifest: SemanticManifest) -> None:  # noqa: D
#         self._saved_query_index: Dict[SavedQueryReference, SavedQuery] = {}
#         for saved_query in semantic_manifest.saved_queries:
#             saved_query_reference = SavedQueryReference(saved_query.name)
#             if saved_query_reference in self._saved_query_index:
#                 raise ValueError(f"Found multiple saved queries with the same reference: {saved_query_reference}")
#             self._saved_query_index[saved_query_reference] = saved_query
#
#     def get_saved_query(self, saved_query_reference: SavedQueryReference) -> SavedQuery:
#         return self._saved_query_index[saved_query_reference]
#
#
# class MetricLookup:
#     def __init__(self, semantic_manifest: SemanticManifest) -> None:  # noqa: D
#         self._metric_index: Dict[MetricReference, Metric] = {}
#         for metric in sorted(semantic_manifest.metrics, key=lambda metric_: metric_.name):
#             metric_reference = metric.reference
#             if metric_reference in self._metric_index:
#                 raise ValueError(f"Found multiple metrics with the same reference: {metric_reference}")
#             self._metric_index[metric_reference] = metric
#
#     def get_metric(self, metric_reference: MetricReference) -> Metric:
#         return self._metric_index[metric_reference]
#
#     @property
#     def metrics(self) -> Sequence[Metric]:
#         return tuple(self._metric_index.values())
