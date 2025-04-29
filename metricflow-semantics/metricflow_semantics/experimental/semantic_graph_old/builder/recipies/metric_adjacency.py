# from __future__ import annotations
#
# from dataclasses import dataclass
# from typing import Dict, Set, Type, Tuple, Iterable
#
# from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
# from dbt_semantic_interfaces.protocols import SemanticManifest
# from dbt_semantic_interfaces.references import MetricReference, MeasureReference
# from dbt_semantic_interfaces.type_enums import TimeGranularity, MetricType, DimensionType
#
# from metricflow_semantics.collection_helpers.merger import Mergeable, MergeableT
# from metricflow_semantics.experimental.semantic_graph_old.graph_nodes import SemanticEntityType
# from metricflow_semantics.experimental.semantic_graph_old.graph_path.path_property import LeftSource
# from metricflow_semantics.experimental.semantic_graph_old.ids.attribute_ids import AttributeId, MetricAttributeId, \
#     DimensionAttributeId, EntityKeyAttributeId
# from metricflow_semantics.experimental.semantic_graph_old.ids.entity_ids import SemanticModelEntityId
# from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup
#
#
# @dataclass
# class MetricAdjacencyResult(Mergeable):
#     metric_to_measures: Dict[MetricReference, Set[MeasureReference]]
#     left_sources: Set[LeftSource]
#     adjacent_non_virtual_entity_ids: Set[SemanticModelEntityId]
#     adjacent_virtual_entity_ids: Set[SemanticModelEntityId]
#     adjacent_entity_id_to_max_defined_grain: Dict[SemanticModelEntityId, TimeGranularity]
#     adjacent_attribute_ids: Set[AttributeId]
#
#     def merge(self, other: MetricAdjacencyResult) -> MetricAdjacencyResult:
#         metric_to_measures = {**self.metric_to_measures, **other.metric_to_measures}
#         left_sources = self.left_sources.union(other.left_sources)
#         adjacent_non_virtual_entity_ids = self.adjacent_non_virtual_entity_ids.intersection(other.adjacent_non_virtual_entity_ids)
#         adjacent_virtual_entity_ids = self.adjacent_virtual_entity_ids.intersection(other.adjacent_virtual_entity_ids)
#
#         adjacent_entity_id_to_max_defined_grain = dict(**self.adjacent_entity_id_to_max_defined_grain)
#         for other_entity_id, other_max_grain in other.adjacent_entity_id_to_max_defined_grain.items():
#
#             current_max_grain = adjacent_entity_id_to_max_defined_grain.get(other_entity_id)
#
#             if current_max_grain is None:
#                 adjacent_entity_id_to_max_defined_grain[other_entity_id] = other_max_grain
#                 continue
#
#             if current_max_grain.to_int() < other_max_grain.to_int():
#                 adjacent_entity_id_to_max_defined_grain[other_entity_id] = other_max_grain
#
#         adjacent_attributes = self.adjacent_attribute_ids.intersection(other.adjacent_attribute_ids)
#
#         return MetricAdjacencyResult(
#             metric_to_measures=metric_to_measures,
#             left_sources=left_sources,
#             adjacent_non_virtual_entity_ids=adjacent_non_virtual_entity_ids,
#             adjacent_virtual_entity_ids=adjacent_virtual_entity_ids,
#             adjacent_entity_id_to_max_defined_grain=adjacent_entity_id_to_max_defined_grain,
#             adjacent_attribute_ids=adjacent_attributes
#         )
#
#     @classmethod
#     def empty_instance(cls: Type[MergeableT]) -> MetricAdjacencyResult:
#         return MetricAdjacencyResult(
#             metric_to_measures={},
#             left_sources=set(),
#             adjacent_non_virtual_entity_ids=set(),
#             adjacent_virtual_entity_ids=set(),
#             adjacent_entity_id_to_max_defined_grain={},
#             adjacent_attribute_ids=set(),
#         )
#
#
# class MetricAdjacencyLookup:
#     """TODO: Make thread safe."""
#
#     def __init__(self, semantic_manifest: SemanticManifest, semantic_model_lookup: SemanticModelLookup) -> None:
#         self._semantic_manifest = semantic_manifest
#         self._semantic_model_lookup = semantic_model_lookup
#         self._attribute_id_to_metric = {
#             MetricAttributeId.get_instance(metric.name): metric for metric in semantic_manifest.metrics
#         }
#         self._attribute_id_to_adjacency_result: Dict[MetricAttributeId, MetricAdjacencyResult] = {}
#         self._measure_to_metric_time_grain: Dict[MeasureReference, TimeGranularity] = {}
#
#     def resolve_adjacency(self, attribute_ids: Iterable[MetricAttributeId]) -> MetricAdjacencyResult:
#         adjacency_results = []
#         for attribute_id in attribute_ids:
#             adjacency_results.append(
#                 self._resolve_adjacency_for_one_metric(
#                     attribute_id
#                 )
#             )
#
#         return MetricAdjacencyResult.merge_iterable(adjacency_results)
#
#     def _resolve_adjacency_for_one_metric(self, attribute_id: MetricAttributeId) -> MetricAdjacencyResult:
#         if attribute_id in self._attribute_id_to_adjacency_result:
#             return self._attribute_id_to_adjacency_result[attribute_id]
#
#         assert attribute_id in self._attribute_id_to_metric, f"Could not find {attribute_id} in {self._attribute_id_to_metric}"
#
#         metric = self._attribute_id_to_metric[attribute_id]
#
#         # Recursive / derived metric case.
#         if len(metric.input_metrics) > 0:
#             # adjacent_non_virtual_entity_ids = set()
#             # metric_to_measures: Dict[MetricReference, Set[MeasureReference]] = {}
#             # left_sources: Set[LeftSource] = set()
#             # measure_references_for_current_metric: Set[MeasureReference] = set()
#             #
#             # adjacent_entity_id_to_valid_grains: Dict[SemanticModelEntityId, Set[TimeGranularity]] = {}
#             #
#             # for input_metric in metric.input_metrics:
#             #     adjacency_result = self.resolve_adjacency(MetricAttributeId.get_instance(input_metric.name))
#             #     adjacent_non_virtual_entity_ids.update(adjacency_result.adjacent_virtual_entity_ids)
#             #     metric_to_measures.update(adjacency_result.metric_to_measures)
#             #     left_sources.update(adjacency_result.left_sources)
#             #     for measure_references_for_parent_metric in adjacency_result.metric_to_measures.values():
#             #         measure_references_for_current_metric.update(measure_references_for_parent_metric)
#             #     adjacent_entity_id_to_valid_grains.update(adjacency_result.adjacent_entity_id_to_max_defined_grain)
#             #
#             # metric_to_measures[MetricReference(metric.name)] = measure_references_for_current_metric
#             # adjacency_result_to_return = MetricAdjacencyResult(
#             #     metric_to_measures=metric_to_measures,
#             #     left_sources=left_sources,
#             #     adjacent_virtual_entity_ids=adjacent_non_virtual_entity_ids,
#             #     adjacent_entity_id_to_valid_grains=adjacent_entity_id_to_valid_grains,
#             # )
#             # self._attribute_id_to_adjacency_result[attribute_id] = adjacency_result_to_return
#             # return adjacency_result_to_return
#             adjacency_results = []
#             for input_metric in metric.input_metrics:
#                 adjacency_results.append(
#                     self.resolve_adjacency(MetricAttributeId.get_instance(input_metric.name))
#                 )
#
#             adjacency_result_to_return = MetricAdjacencyResult.merge_iterable(adjacency_results)
#             self._attribute_id_to_adjacency_result[attribute_id] = adjacency_result_to_return
#             return adjacency_result_to_return
#
#         # Base metric case.
#         measure_references_for_metric: Tuple[MeasureReference, ...]
#         if metric.type is MetricType.CONVERSION:
#             conversion_type_params = metric.type_params.conversion_type_params
#             assert (
#                 conversion_type_params
#             ), "A conversion metric should have type_params.conversion_type_params defined."
#             measure_references_for_metric = (conversion_type_params.base_measure.measure_reference,)
#         else:
#             measure_references_for_metric = tuple(
#                 input_measure.measure_reference for input_measure in metric.input_measures
#             )
#
#         if len(measure_references_for_metric) == 0:
#             raise RuntimeError(f"No measures found for {MetricReference(metric.name)}")
#
#         adjacency_results = []
#         for measure_reference in measure_references_for_metric:
#             adjacency_results.append(
#                 self._get_adjacency_result_for_measure(
#                     metric_reference=MetricReference(metric.name),
#                     measure_reference=measure_reference,
#                 )
#             )
#
#         adjacency_result_to_return = MetricAdjacencyResult.merge_iterable(adjacency_results)
#         self._attribute_id_to_adjacency_result[attribute_id] = adjacency_result_to_return
#         return adjacency_result_to_return
#
#     def _get_adjacency_result_for_measure(self, metric_reference: MetricReference, measure_reference: MeasureReference) -> MetricAdjacencyResult:
#         adjacent_non_virtual_entity_ids: Set[SemanticModelEntityId] = set()
#         left_sources: Set[LeftSource] = set()
#
#         semantic_model = self._semantic_model_lookup.get_semantic_model_for_measure(measure_reference)
#         left_sources.add(LeftSource(measure_reference, semantic_model.reference))
#         for entity in semantic_model.entities:
#             adjacent_non_virtual_entity_ids.add(
#                 SemanticModelEntityId.get_instance(entity.name, SemanticEntityType.ENTITY)
#             )
#         primary_entity_name = semantic_model.primary_entity
#         if primary_entity_name is not None:
#             adjacent_non_virtual_entity_ids.add(
#                 SemanticModelEntityId.get_instance(primary_entity_name, SemanticEntityType.ENTITY)
#             )
#
#         adjacent_entity_id_to_max_defined_grain: Dict[SemanticModelEntityId, TimeGranularity] = {}
#         metric_time_grain = self._get_metric_time_grain_for_measure(measure_reference)
#         adjacent_entity_id_to_max_defined_grain[
#             SemanticModelEntityId.get_instance(
#                 element_name=METRIC_TIME_ELEMENT_NAME,
#                 entity_type=SemanticEntityType.TIME_DIMENSION
#             )
#         ] = metric_time_grain
#
#         adjacent_entity_id_to_max_defined_grain: Dict[SemanticModelEntityId, TimeGranularity]
#         adjacent_attribute_ids: Set[AttributeId] = set()
#         for dimension in semantic_model.dimensions:
#             if dimension.type is DimensionType.TIME:
#                 assert dimension.type_params is not None
#                 entity_id = SemanticModelEntityId.get_instance(dimension.name, SemanticEntityType.TIME_DIMENSION)
#                 adjacent_non_virtual_entity_ids.add(entity_id)
#                 adjacent_entity_id_to_max_defined_grain[entity_id] = dimension.type_params.time_granularity
#             else:
#                 adjacent_attribute_ids.add(
#                     DimensionAttributeId.get_instance(dimension.name)
#                 )
#         for entity in semantic_model.entities:
#             adjacent_attribute_ids.add(EntityKeyAttributeId.get_instance(entity.name))
#
#         adjacent_virtual_entity_ids: Set[SemanticModelEntityId] = set()
#         if semantic_model.primary_entity is not None:
#             adjacent_virtual_entity_ids.add(
#                 SemanticModelEntityId.get_instance(semantic_model.primary_entity, SemanticEntityType.ENTITY)
#             )
#         return MetricAdjacencyResult(
#             metric_to_measures={metric_reference: {measure_reference}},
#             left_sources=left_sources,
#             adjacent_non_virtual_entity_ids=adjacent_non_virtual_entity_ids,
#             adjacent_virtual_entity_ids=adjacent_virtual_entity_ids,
#             adjacent_entity_id_to_max_defined_grain=adjacent_entity_id_to_max_defined_grain,
#             adjacent_attribute_ids=adjacent_attribute_ids
#         )
#
#     def _get_metric_time_grain_for_measure(self, measure_reference: MeasureReference) -> TimeGranularity:
#         if measure_reference in self._measure_to_metric_time_grain:
#             return self._measure_to_metric_time_grain[measure_reference]
#
#         semantic_model = self._semantic_model_lookup.get_semantic_model_for_measure(measure_reference)
#         agg_time_dimension_reference = semantic_model.checked_agg_time_dimension_for_measure(semantic_model)
#         for dimension in semantic_model.dimensions:
#             if dimension.time_dimension_reference == agg_time_dimension_reference:
#                 if dimension.type_params is None:
#                     raise RuntimeError(
#                         f"{measure_reference} is to be aggregated by {agg_time_dimension_reference}, but "
#                         f"{dimension.type_params=}. This should have caught during validation."
#                     )
#                 metric_time_grain = dimension.type_params.time_granularity
#                 self._measure_to_metric_time_grain[measure_reference] = metric_time_grain
#                 return metric_time_grain
