# from __future__ import annotations
#
# import logging
# from collections import defaultdict
# from random import Random
#
# from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
# from dbt_semantic_interfaces.protocols import SemanticManifest
# from dbt_semantic_interfaces.references import MetricReference
# from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
# from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
# from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
# from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
# from metricflow_semantics.protocols.query_parameter import GroupByQueryParameter
# from metricflow_semantics.specs.query_param_implementations import (
#     DimensionOrEntityParameter,
#     MetricParameter,
#     TimeDimensionParameter,
# )
# from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
#
# from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
#
# logger = logging.getLogger(__name__)
#
#
# @fast_frozen_dataclass()
# class RandomQueryDescriptor:
#     max_metric_count: int
#     max_group_by_item_count: int
#
#     group_by_dimension_weight: int
#     group_by_entity_weight: int
#
#     random_seed: int
#
#
# class RandomQueryGenerator:
#     def __init__(self, semantic_manifest: SemanticManifest, random_query_descriptor: RandomQueryDescriptor) -> None:
#         self._manifest = semantic_manifest
#         self._manifest_lookup = SemanticManifestLookup(semantic_manifest)
#         self._query_descriptor = random_query_descriptor
#         self._random = Random(random_query_descriptor.random_seed)
#         self._available_metric_references = tuple(MetricReference(metric.name) for metric in semantic_manifest.metrics)
#
#     def generate(self) -> MetricFlowQueryRequest:
#         metric_count = self._random.randint(1, self._query_descriptor.max_metric_count)
#         metric_parameters = tuple(
#             MetricParameter(metric_reference.element_name)
#             for metric_reference in self._random.choices(
#                 self._available_metric_references, k=self._query_descriptor.max_metric_count
#             )
#         )
#
#         total_group_by_item_weight = (
#             self._query_descriptor.group_by_dimension_weight,
#             self._query_descriptor.group_by_entity_weight,
#             self._query_descriptor.group_by_metric_weight,
#         )
#
#         group_by_item_count = self._random.randint(1, self._query_descriptor.max_group_by_item_count)
#         element_types = (
#             LinkableElementType.DIMENSION,
#             LinkableElementType.ENTITY,
#             LinkableElementType.METRIC,
#         )
#
#         selected_element_types = self._random.sample(
#             element_types,
#             counts=[
#                 self._query_descriptor.group_by_dimension_weight,
#                 self._query_descriptor.group_by_entity_weight,
#                 self._query_descriptor.group_by_metric_weight,
#             ],
#             k=group_by_item_count,
#         )
#
#         available_group_by_set = self._manifest_lookup.metric_lookup.get_common_group_by_items(
#             self._available_metric_references,
#             GroupByItemSetFilter.create(),
#         )
#
#         dimension_parameters = []
#         time_dimension_parameters = []
#         entity_parameters = []
#
#         element_type_to_parameters: defaultdict[LinkableElementType, list[GroupByQueryParameter]] = []
#         for annotated_spec in available_group_by_set.annotated_specs:
#             element_type = annotated_spec.element_type
#             dunder_name = StructuredLinkableSpecName(
#                 entity_link_names=annotated_spec.entity_link_names,
#                 element_name=annotated_spec.element_name,
#             ).dunder_name
#             if element_type is LinkableElementType.DIMENSION or element_type is LinkableElementType.ENTITY:
#                 # dimension_parameters.append(DimensionOrEntityParameter(dunder_name))
#                 element_type_to_parameters[element_type].append(DimensionOrEntityParameter(dunder_name))
#             elif element_type is LinkableElementType.TIME_DIMENSION:
#                 # time_dimension_parameters.append(
#                 #     TimeDimensionParameter(
#                 #         dunder_name,
#                 #         grain=annotated_spec.time_grain.name,
#                 #     )
#                 # )
#                 element_type_to_parameters[element_type].append(
#                     TimeDimensionParameter(
#                         dunder_name,
#                         grain=annotated_spec.time_grain.name,
#                     )
#                 )
#             elif element_type is LinkableElementType.METRIC:
#                 # Group-by-metrics are only used in filters.
#                 pass
#             else:
#                 assert_values_exhausted(element_type)
#
#         # group_by_parameters: list[GroupByQueryParameter] = []
#
#         # for element_type in selected_element_types:
#         #     if element_type is LinkableElementType.DIMENSION:
#         #         set_filter = GroupByItemSetFilter.create(
#         #             any_properties_denylist=[GroupByItemProperty.ENTITY, GroupByItemProperty.METRIC],
#         #         )
#         #     elif element_type is LinkableElementType.ENTITY:
#         #         set_filter = GroupByItemSetFilter.create(
#         #             any_properties_allowlist=[GroupByItemProperty.ENTITY]
#         #         )
#         #     elif element_type is LinkableElementType.METRIC:
#         #         set_filter = GroupByItemSetFilter.create(
#         #             any_properties_allowlist=[GroupByItemProperty.ENTITY],
#         #         )
#         #
#         #     available_group_by_set = self._manifest_lookup.metric_lookup.get_common_group_by_items(
#         #         self._available_metric_references,
#         #         set_filter=
#         #     )
#         group_by_parameters: list[GroupByQueryParameter] = []
#         for element_type in selected_element_types:
#             raise RuntimeError
