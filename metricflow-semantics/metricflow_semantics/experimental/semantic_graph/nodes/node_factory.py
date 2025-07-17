# from __future__ import annotations
#
# import logging
#
# from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
# from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
# from metricflow_semantics.experimental.semantic_graph.manifest_object_lookup import ManifestObjectLookup
# from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import MetricNode
# from metricflow_semantics.experimental.semantic_graph.nodes.node_label import RequireMetricTimeInQueryLabel
#
# logger = logging.getLogger(__name__)


# class SemanticGraphNodeFactory:
#     def __init__(self, manifest_object_lookup: ManifestObjectLookup) -> None:
#         self._lookup = manifest_object_lookup
#
#     def get_metric_node(self, metric_name: str) -> MetricNode:
#         return self._get_metric_node(metric_name)
#
#     def _get_metric_node(self, metric_name: str) -> MetricNode:
#         metric = self._lookup.get_metric(metric_name)
#
#         extra_labels: AnyLengthTuple[MetricflowGraphLabel] = ()
#
#         if metric.type_params.cumulative_type_params and (
#             metric.type_params.cumulative_type_params.window is not None
#             or metric.type_params.cumulative_type_params.grain_to_date is not None
#         ):
#             extra_labels = (RequireMetricTimeInQueryLabel.get_instance(),)
#
#         return MetricNode.get_instance(
#             metric_name=metric_name,
#             extra_labels=extra_labels,
#         )
