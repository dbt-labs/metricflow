from __future__ import annotations

import logging
from enum import Enum

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols import Metric
from dbt_semantic_interfaces.type_enums import MetricType
from typing_extensions import override

from metricflow_semantics.experimental.dsi.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.metricflow_exception import InvalidManifestException
from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_recipe_step import (
    AttributeRecipeStep,
)
from metricflow_semantics.experimental.semantic_graph.builder.subgraph_generator import (
    SemanticSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.edges.edge_labels import (
    CumulativeMeasureLabel,
    DenyDatePartLabel,
    DenyEntityKeyQueryResolutionLabel,
    DenyVisibleAttributesLabel,
)
from metricflow_semantics.experimental.semantic_graph.edges.sg_edges import MetricDefinitionEdge
from metricflow_semantics.experimental.semantic_graph.nodes.entity_nodes import (
    BaseMetricNode,
    DerivedMetricNode,
    MeasureNode,
    MetricNode,
)
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


class _SpecialCase(Enum):
    """Enumerates the special cases that affect the available group-by items for a metric."""

    CONVERSION_MEASURE = "conversion_measure"
    CUMULATIVE_METRIC = "cumulative_metric"
    CUMULATIVE_METRIC_WITH_WINDOW_OR_GRAIN_TO_DATE = "cumulative_metric_with_window_or_grain_to_date"
    TIME_OFFSET_DERIVED_METRIC = "time_offset_derived_metric"


class MetricSubgraphGenerator(SemanticSubgraphGenerator):
    """Generates the subgraph that models the relationship between metrics.

    * The successors of base-metric nodes are measure nodes.
    * The successors of derived-metric nodes are other metric nodes (base or derived).
    """

    @override
    def __init__(self, manifest_object_lookup: ManifestObjectLookup) -> None:
        super().__init__(manifest_object_lookup)
        self._verbose_debug_logs = False

        # Maps the metric name to the corresponding metric node that was generated.
        self._metric_name_to_node: dict[str, MetricNode] = {}
        self._empty_edge_labels: FrozenOrderedSet[MetricflowGraphLabel] = FrozenOrderedSet()
        self._empty_recipe_step = AttributeRecipeStep()

        common_cumulative_metric_labels = FrozenOrderedSet(
            (
                CumulativeMeasureLabel.get_instance(),
                DenyDatePartLabel.get_instance(),
            )
        )

        # Maps the special cases to the labels that should be associated with the edge that connects a metric node
        # to successor nodes.
        self._special_case_to_successor_edge_label = {
            _SpecialCase.CONVERSION_MEASURE: FrozenOrderedSet((DenyVisibleAttributesLabel.get_instance(),)),
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

    def _add_edges_for_base_metric(
        self,
        base_metric: Metric,
        metric_name_to_node: dict[str, SemanticGraphNode],
        edge_list: list[SemanticGraphEdge],
    ) -> None:
        """Adds the edges from a base-metric node to the measure nodes."""
        if len(base_metric.input_metrics) > 0:
            raise RuntimeError(
                LazyFormat("This method should have been called with metrics that do not have any input metrics.")
            )

        input_measures = base_metric.input_measures
        if len(input_measures) == 0:
            raise InvalidManifestException(
                LazyFormat(
                    "The given base metric does not have any input measures.",
                    base_metric=base_metric,
                )
            )

        measure_name_to_labels_for_metric_to_measure_edge: dict[str, FrozenOrderedSet[MetricflowGraphLabel]] = {}
        recipe_step = self._empty_recipe_step
        metric_type = base_metric.type
        if metric_type is MetricType.SIMPLE or metric_type is MetricType.RATIO or metric_type is MetricType.DERIVED:
            pass
        elif metric_type is MetricType.CUMULATIVE:
            # Cumulative metrics impose special restrictions on the group-by items available, so label those edges
            # appropriately.
            recipe_step = AttributeRecipeStep(set_deny_date_part=True)
            if base_metric.type_params.cumulative_type_params and (
                base_metric.type_params.cumulative_type_params.window is not None
                or base_metric.type_params.cumulative_type_params.grain_to_date is not None
            ):
                edge_labels = self._special_case_to_successor_edge_label[
                    _SpecialCase.CUMULATIVE_METRIC_WITH_WINDOW_OR_GRAIN_TO_DATE
                ]
            else:
                edge_labels = self._special_case_to_successor_edge_label[_SpecialCase.CUMULATIVE_METRIC]

            for measure in base_metric.input_measures:
                measure_name_to_labels_for_metric_to_measure_edge[measure.name] = edge_labels
        elif metric_type is MetricType.CONVERSION:
            # Label the edge for conversion measures as conversion measures need to be handled as a special case when
            # resolving the associated group-by items.
            conversion_type_params = base_metric.type_params.conversion_type_params
            if conversion_type_params is not None:
                conversion_measure_name = conversion_type_params.conversion_measure.name
                measure_name_to_labels_for_metric_to_measure_edge[
                    conversion_measure_name
                ] = self._special_case_to_successor_edge_label[_SpecialCase.CONVERSION_MEASURE]
            else:
                raise InvalidManifestException(
                    LazyFormat(
                        "A conversion metric is missing type parameters",
                        base_metric=base_metric,
                    )
                )
        else:
            assert_values_exhausted(metric_type)

        base_metric_node = BaseMetricNode.get_instance(base_metric.name)

        for measure in base_metric.input_measures:
            measure_name = measure.name
            source_model_id = self._manifest_object_lookup.get_model_id_for_measure(measure_name)

            head_node = MeasureNode.get_instance(
                measure_name=measure_name,
                source_model_id=source_model_id,
            )

            edge_list.append(
                MetricDefinitionEdge.create(
                    tail_node=base_metric_node,
                    head_node=head_node,
                    additional_labels=measure_name_to_labels_for_metric_to_measure_edge.get(measure_name),
                    recipe_step=recipe_step,
                )
            )

        metric_name_to_node[base_metric.name] = base_metric_node

    def _add_edges_for_derived_metric(
        self,
        derived_metric: Metric,
        metric_name_to_node: dict[str, SemanticGraphNode],
        edge_list: list[SemanticGraphEdge],
    ) -> None:
        """Adds the edges from a derived-metric node to the nodes associated with the input metrics."""
        input_metrics = derived_metric.input_metrics

        if len(input_metrics) == 0:
            raise RuntimeError(
                LazyFormat(
                    "This method should have been called with a metric that has input metrics",
                    parent_input_metrics=input_metrics,
                )
            )

        derived_metric_node = DerivedMetricNode.get_instance(derived_metric.name)
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
                tail_node=derived_metric_node, head_node=input_metric_node, additional_labels=additional_edge_labels
            )

            edge_list.append(edge_to_add)
        metric_name_to_node[derived_metric.name] = derived_metric_node

    def _add_edges_for_any_metric(
        self,
        metric: Metric,
        metric_name_to_node: dict[str, SemanticGraphNode],
        edge_list: list[SemanticGraphEdge],
    ) -> None:
        """Adds edges for any type of metric."""
        if len(metric.input_metrics) > 0:
            self._add_edges_for_derived_metric(
                derived_metric=metric,
                metric_name_to_node=metric_name_to_node,
                edge_list=edge_list,
            )
        else:
            self._add_edges_for_base_metric(
                base_metric=metric,
                metric_name_to_node=metric_name_to_node,
                edge_list=edge_list,
            )
