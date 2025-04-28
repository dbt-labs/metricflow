# from __future__ import annotations
#
# import logging
# from typing import Sequence
#
# from dbt_semantic_interfaces.protocols import Metric
# from dbt_semantic_interfaces.references import MetricReference
#
# from metricflow_semantics.experimental.semantic_graph.builder.in_progress_semantic_graph import InProgressSemanticGraph
# from metricflow_semantics.experimental.semantic_graph.builder.semantic_graph_transform_rule import (
#     SemanticGraphRecipe,
# )
# from metricflow_semantics.experimental.semantic_graph.computation_method import (
#     MetricComputationMethod,
# )
# from metricflow_semantics.experimental.semantic_graph.graph_edges import (
#     ProvidedEdgeTagSet,
#     RequiredTagSet,
#     SemanticGraphEdgeType,
# )
# from metricflow_semantics.experimental.semantic_graph.graph_nodes import (
#     MeasureNode, MetricAttributeNode,
# )
# from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
#
# logger = logging.getLogger(__name__)
#
#
# class AddMetricNodesRecipe(SemanticGraphRecipe):
#
#     def _get_input_metrics(self, metric: Metric) -> Sequence[MetricReference]:
#         input_metrics = []
#         if metric.type_params.numerator is not None:
#             input_metrics.append(metric.type_params.numerator.as_reference)
#         if metric.type_params.denominator is not None:
#             input_metrics.append(metric.type_params.denominator.as_reference)
#         if metric.type_params.metrics is not None:
#             input_metrics.extend(metric.as_reference for metric in metric.type_params.metrics)
#         return input_metrics
#
#     def execute_recipe(self, semantic_graph: InProgressSemanticGraph) -> None:
#         for metric in self._semantic_manifest.metrics:
#             logger.info(LazyFormat("Adding metric.", metric=metric))
#             semantic_graph.add_node(MetricAttributeNode(MetricReference(metric.name)))
#
#         for metric in self._semantic_manifest.metrics:
#             metric_input_measure = metric.type_params.measure
#
#             metric_node = MetricAttributeNode(MetricReference(metric.name))
#
#             if metric_input_measure is not None:
#                 semantic_graph.add_edge(
#                     tail_node=MeasureNode(metric_input_measure.measure_reference),
#                     edge_type=SemanticGraphEdgeType.METRIC_ATTRIBUTE,
#                     head_node=metric_node,
#                     computation_method=MetricComputationMethod(),
#                     required_tags=RequiredTagSet.empty_set(),
#                     provided_tags=ProvidedEdgeTagSet.empty_set(),
#                 )
#                 semantic_graph.add_edge(
#                     tail_node=metric_node,
#                     edge_type=SemanticGraphEdgeType.ATTRIBUTE_SOURCE,
#                     head_node=MeasureNode(metric_input_measure.measure_reference),
#                     computation_method=MetricComputationMethod(),
#                     required_tags=RequiredTagSet.empty_set(),
#                     provided_tags=ProvidedEdgeTagSet.empty_set(),
#                 )
#
#             input_metrics = metric.input_metrics
#             if input_metrics is not None:
#                 for input_metrics in input_metrics:
#                     metric_input_node = MetricAttributeNode(input_metrics.as_reference)
#                     semantic_graph.add_edge(
#                         tail_node=metric_input_node,
#                         edge_type=SemanticGraphEdgeType.METRIC_ATTRIBUTE,
#                         head_node=metric_node,
#                         computation_method=MetricComputationMethod(),
#                         required_tags=RequiredTagSet.empty_set(),
#                         provided_tags=ProvidedEdgeTagSet.empty_set(),
#                     )
#
#                     semantic_graph.add_edge(
#                         tail_node=metric_node,
#                         edge_type=SemanticGraphEdgeType.ATTRIBUTE_SOURCE,
#                         head_node=metric_input_node,
#                         computation_method=MetricComputationMethod(),
#                         required_tags=RequiredTagSet.empty_set(),
#                         provided_tags=ProvidedEdgeTagSet.empty_set(),
#                     )
