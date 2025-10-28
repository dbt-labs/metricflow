from __future__ import annotations

import logging
from enum import Enum

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols import Metric
from dbt_semantic_interfaces.type_enums import MetricType
from typing_extensions import override

from metricflow_semantics.errors.error_classes import InvalidManifestException
from metricflow_semantics.model.semantics.metric_lookup import MetricLookup
from metricflow_semantics.semantic_graph.attribute_resolution.attribute_recipe_step import (
    AttributeRecipeStep,
)
from metricflow_semantics.semantic_graph.builder.subgraph_generator import (
    SemanticSubgraphGenerator,
)
from metricflow_semantics.semantic_graph.edges.edge_labels import (
    CumulativeMetricLabel,
    DenyDatePartLabel,
    DenyEntityKeyQueryResolutionLabel,
    DenyVisibleAttributesLabel,
)
from metricflow_semantics.semantic_graph.edges.sg_edges import MetricDefinitionEdge
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.semantic_graph.nodes.entity_nodes import (
    ComplexMetricNode,
    MetricNode,
    SimpleMetricNode,
)
from metricflow_semantics.semantic_graph.sg_interfaces import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet
from metricflow_semantics.toolkit.mf_graph.graph_labeling import MetricFlowGraphLabel
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


class _SpecialCase(Enum):
    """Enumerates the special cases that affect the available group-by items for a metric."""

    CONVERSION_INPUT_METRIC = "conversion_input_metric"
    CUMULATIVE_METRIC = "cumulative_metric"
    CUMULATIVE_METRIC_WITH_WINDOW_OR_GRAIN_TO_DATE = "cumulative_metric_with_window_or_grain_to_date"
    TIME_OFFSET_DERIVED_METRIC = "time_offset_derived_metric"


class ComplexMetricSubgraphGenerator(SemanticSubgraphGenerator):
    """Generates the subgraph that models the relationship between complex metrics.

    * The successors of simple-metric nodes are metric time nodes / local-model nodes.
    * The successors of complex-metric nodes are other metric nodes (complex or simple).
    """

    @override
    def __init__(self, manifest_object_lookup: ManifestObjectLookup) -> None:
        super().__init__(manifest_object_lookup)
        self._verbose_debug_logs = False

        # Maps the metric name to the corresponding metric node that was generated.
        self._metric_name_to_node: dict[str, MetricNode] = {}
        self._empty_edge_labels: FrozenOrderedSet[MetricFlowGraphLabel] = FrozenOrderedSet()
        self._empty_recipe_step = AttributeRecipeStep()

        common_cumulative_metric_labels = FrozenOrderedSet(
            (
                CumulativeMetricLabel.get_instance(),
                DenyDatePartLabel.get_instance(),
            )
        )

        # Maps the special cases to the labels that should be associated with the edge that connects a metric node
        # to successor nodes.
        self._special_case_to_successor_edge_label = {
            _SpecialCase.CONVERSION_INPUT_METRIC: FrozenOrderedSet((DenyVisibleAttributesLabel.get_instance(),)),
            _SpecialCase.CUMULATIVE_METRIC: common_cumulative_metric_labels,
            _SpecialCase.CUMULATIVE_METRIC_WITH_WINDOW_OR_GRAIN_TO_DATE: common_cumulative_metric_labels.union(
                (DenyEntityKeyQueryResolutionLabel.get_instance(),)
            ),
            _SpecialCase.TIME_OFFSET_DERIVED_METRIC: FrozenOrderedSet(
                (DenyEntityKeyQueryResolutionLabel.get_instance(),)
            ),
        }

    @override
    def add_edges_for_manifest(self, edge_list: list[SemanticGraphEdge]) -> None:
        for metric in self._manifest_object_lookup.get_metrics():
            self._add_edges_for_any_metric(
                metric=metric,
                metric_name_to_node={},
                edge_list=edge_list,
            )

    def _add_edges_for_complex_metric(
        self,
        complex_metric: Metric,
        metric_name_to_node: dict[str, SemanticGraphNode],
        edge_list: list[SemanticGraphEdge],
    ) -> None:
        """Adds the edges from a derived-metric node to the nodes associated with the input metrics."""
        metric_type = complex_metric.type

        input_metric_name_to_labels_for_metric_to_input_metric_edge: dict[
            str, FrozenOrderedSet[MetricFlowGraphLabel]
        ] = {}
        recipe_step = self._empty_recipe_step

        input_metrics = MetricLookup.metric_inputs(complex_metric, include_conversion_metric_input=True)
        if metric_type is MetricType.SIMPLE:
            pass
        elif metric_type is MetricType.CUMULATIVE:
            cumulative_type_params = complex_metric.type_params.cumulative_type_params
            if cumulative_type_params is None:
                raise InvalidManifestException(
                    LazyFormat(
                        "Expected `cumulative_type_params` to be set for a cumulative metric",
                        complex_metric=complex_metric,
                    )
                )

            # Cumulative metrics impose special restrictions on the group-by items available, so label those edges
            # appropriately.

            recipe_step = AttributeRecipeStep(set_deny_date_part=True)
            if cumulative_type_params.window is not None or cumulative_type_params.grain_to_date is not None:
                edge_labels = self._special_case_to_successor_edge_label[
                    _SpecialCase.CUMULATIVE_METRIC_WITH_WINDOW_OR_GRAIN_TO_DATE
                ]
            else:
                edge_labels = self._special_case_to_successor_edge_label[_SpecialCase.CUMULATIVE_METRIC]

            input_metric_for_cumulative_metric = cumulative_type_params.metric
            if input_metric_for_cumulative_metric is None:
                raise InvalidManifestException(
                    LazyFormat(
                        "Expected `metric` to be set for a cumulative metric",
                        complex_metric=complex_metric,
                    )
                )

            input_metric_name_to_labels_for_metric_to_input_metric_edge[
                input_metric_for_cumulative_metric.name
            ] = edge_labels
        elif metric_type is MetricType.RATIO:
            pass
        elif metric_type is MetricType.CONVERSION:
            conversion_type_params = complex_metric.type_params.conversion_type_params
            if conversion_type_params is None:
                raise InvalidManifestException(
                    LazyFormat(
                        "Expected `conversion_type_params` to be set for a conversion metric",
                        complex_metric=complex_metric,
                    )
                )

            conversion_metric = conversion_type_params.conversion_metric
            if conversion_metric is None:
                raise InvalidManifestException(
                    LazyFormat(
                        "Expected `conversion_metric` to be set for a conversion metric", complex_metric=complex_metric
                    )
                )

            input_metric_name_to_labels_for_metric_to_input_metric_edge[
                conversion_metric.name
            ] = self._special_case_to_successor_edge_label[_SpecialCase.CONVERSION_INPUT_METRIC]

        elif metric_type is MetricType.DERIVED:
            pass
        else:
            assert_values_exhausted(metric_type)

        if len(input_metrics) == 0:
            raise RuntimeError(
                LazyFormat(
                    "This method should have been called with a metric that has input metrics",
                    complex_metric=complex_metric,
                    parent_input_metrics=input_metrics,
                )
            )

        complex_metric_node = ComplexMetricNode.get_instance(complex_metric.name)
        additional_edge_labels = self._empty_edge_labels

        for input_metric in input_metrics:
            # Add labels for time-offset derived metrics as that is a special case when resolving the associated
            # group-by items.
            if input_metric.offset_window is not None or input_metric.offset_to_grain is not None:
                additional_edge_labels = self._special_case_to_successor_edge_label[
                    _SpecialCase.TIME_OFFSET_DERIVED_METRIC
                ]
                break

        for input_metric in input_metrics:
            input_metric_name = input_metric.name
            if input_metric_name not in metric_name_to_node:
                self._add_edges_for_any_metric(
                    metric=self._manifest_object_lookup.get_metric(input_metric_name),
                    metric_name_to_node=metric_name_to_node,
                    edge_list=edge_list,
                )
            input_metric_node = metric_name_to_node[input_metric_name]

            edge_to_add = MetricDefinitionEdge.create(
                tail_node=complex_metric_node,
                head_node=input_metric_node,
                additional_labels=additional_edge_labels.union(
                    FrozenOrderedSet(
                        input_metric_name_to_labels_for_metric_to_input_metric_edge.get(input_metric.name) or ()
                    )
                ),
                recipe_step=recipe_step,
            )

            edge_list.append(edge_to_add)
        metric_name_to_node[complex_metric.name] = complex_metric_node

    def _add_edges_for_any_metric(
        self,
        metric: Metric,
        metric_name_to_node: dict[str, SemanticGraphNode],
        edge_list: list[SemanticGraphEdge],
    ) -> None:
        """Adds edges for any type of metric.

        Args:
            metric: Add edges for this metric.
            metric_name_to_node: dict to update when a metric node is added.
            edge_list:  list to update when an edge is added.
        """
        metric_type = metric.type

        if metric_type is MetricType.SIMPLE:
            metric_name = metric.name
            metric_name_to_node[metric_name] = SimpleMetricNode.get_instance(metric_name)
        elif (
            metric_type is MetricType.RATIO
            or metric_type is MetricType.CUMULATIVE
            or metric_type is MetricType.CONVERSION
            or metric_type is MetricType.DERIVED
        ):
            self._add_edges_for_complex_metric(
                complex_metric=metric,
                metric_name_to_node=metric_name_to_node,
                edge_list=edge_list,
            )
        else:
            assert_values_exhausted(metric_type)
