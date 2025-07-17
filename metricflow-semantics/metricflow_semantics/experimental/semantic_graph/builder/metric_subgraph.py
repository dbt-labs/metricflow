from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols import Metric
from dbt_semantic_interfaces.type_enums import MetricType
from typing_extensions import override

from metricflow_semantics.experimental.metricflow_exception import InvalidManifestException
from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import AttributeRecipeUpdate
from metricflow_semantics.experimental.semantic_graph.builder.graph_change_rule import (
    SemanticSubgraphGenerator,
    SubgraphGeneratorArgumentSet,
)
from metricflow_semantics.experimental.semantic_graph.edges.entity_relationship import MetricDefinitionEdge
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import MeasureNode
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import (
    BaseMetricNode,
    DerivedMetricNode,
    MetricNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    CumulativeMeasureLabel,
    DenyDatePartLabel,
    DenyEntityKeyQueryResolutionLabel,
    DenyVisibleAttributesLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import SemanticGraphNode
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph, SemanticGraph
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


class _SpecialCase(Enum):
    CONVERSION_MEASURE = "conversion_measure"
    CUMULATIVE_METRIC = "cumulative_metric"
    CUMULATIVE_METRIC_WITH_WINDOW_OR_GRAIN_TO_DATE = "cumulative_metric_with_window_or_grain_to_date"
    TIME_OFFSET_DERIVED_METRIC = "time_offset_derived_metric"


class MetricSubgraphGenerator(SemanticSubgraphGenerator):
    def __init__(self, argument_set: SubgraphGeneratorArgumentSet) -> None:
        super().__init__(argument_set)
        self._verbose_debug_logs = False
        self._finished_metric_names = set[str]()
        self._metric_name_to_node: dict[str, MetricNode] = {}
        self._default_edge_labels: FrozenOrderedSet[MetricflowGraphLabel] = FrozenOrderedSet()

        self._conversion_measure_edge_labels = FrozenOrderedSet((DenyVisibleAttributesLabel.get_instance(),))

        common_cumulative_metric_labels = FrozenOrderedSet(
            (
                CumulativeMeasureLabel.get_instance(),
                DenyDatePartLabel.get_instance(),
            )
        )
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

    def _add_elements_for_base_metric(
        self,
        current_subgraph: MutableSemanticGraph,
        base_metric: Metric,
        metric_name_to_node: dict[str, SemanticGraphNode],
    ) -> None:
        recipe_update = AttributeRecipeUpdate()
        metric_type = base_metric.type

        measure_name_to_labels_for_metric_to_measure_edge: dict[str, FrozenOrderedSet[MetricflowGraphLabel]] = {}

        if metric_type is MetricType.SIMPLE or metric_type is MetricType.RATIO:
            pass
        elif metric_type is MetricType.DERIVED:
            raise RuntimeError(
                LazyFormat(
                    "This method should not have been called with a derived metric",
                    metric=base_metric,
                )
            )
        elif metric_type is MetricType.CUMULATIVE:
            recipe_update = AttributeRecipeUpdate(set_deny_date_part=True)
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
            conversion_type_params = base_metric.type_params.conversion_type_params
            assert conversion_type_params, "A conversion metric should have type_params.conversion_type_params defined."
            conversion_measure_name = conversion_type_params.conversion_measure.name
            measure_name_to_labels_for_metric_to_measure_edge[
                conversion_measure_name
            ] = self._special_case_to_successor_edge_label[_SpecialCase.CONVERSION_MEASURE]

        else:
            assert_values_exhausted(metric_type)

        base_metric_node = BaseMetricNode.get_instance(base_metric.name)

        input_measures = base_metric.input_measures
        if len(input_measures) == 0:
            raise InvalidManifestException(
                LazyFormat(
                    "The given base metric does not have any input measures.",
                    base_metric=base_metric,
                )
            )

        for measure in base_metric.input_measures:
            measure_name = measure.name
            source_model_id = self._manifest_object_lookup.measure_name_to_model_id[measure_name]

            head_node = MeasureNode.get_instance(
                measure_name=measure_name,
                source_model_id=source_model_id,
            )

            current_subgraph.add_edge(
                MetricDefinitionEdge.get_instance(
                    tail_node=base_metric_node,
                    head_node=head_node,
                    additional_labels=measure_name_to_labels_for_metric_to_measure_edge.get(measure_name),
                    recipe_update=recipe_update,
                )
            )

        metric_name_to_node[base_metric.name] = base_metric_node

    def _add_elements_for_derived_metric(
        self,
        current_subgraph: MutableSemanticGraph,
        derived_metric: Metric,
        metric_name_to_node: dict[str, SemanticGraphNode],
    ) -> None:
        parent_metric_inputs = derived_metric.type_params.metrics
        assert parent_metric_inputs is not None, "This should have been called with a derived metric"

        derived_metric_node = DerivedMetricNode.get_instance(derived_metric.name)

        additional_edge_labels = self._default_edge_labels

        if len(parent_metric_inputs) == 0:
            raise InvalidManifestException(
                LazyFormat(
                    "The given derived metric does not have any input metrics.",
                    derived_metric=derived_metric,
                )
            )

        for parent_metric_input in parent_metric_inputs:
            if parent_metric_input.offset_window is not None or parent_metric_input.offset_to_grain is not None:
                additional_edge_labels = self._special_case_to_successor_edge_label[
                    _SpecialCase.TIME_OFFSET_DERIVED_METRIC
                ]
                break

        for parent_metric_input in parent_metric_inputs:
            parent_metric_name = parent_metric_input.name
            if parent_metric_name not in metric_name_to_node:
                self._add_elements_for_any_metric(
                    current_subgraph=current_subgraph,
                    metric=self._manifest_object_lookup.get_metric(parent_metric_name),
                    metric_name_to_node=metric_name_to_node,
                )
            parent_metric_node = metric_name_to_node[parent_metric_name]

            edge_to_add = MetricDefinitionEdge.get_instance(
                tail_node=derived_metric_node, head_node=parent_metric_node, additional_labels=additional_edge_labels
            )

            current_subgraph.add_edge(edge_to_add)
        metric_name_to_node[derived_metric.name] = derived_metric_node

    def _add_elements_for_any_metric(
        self,
        current_subgraph: MutableSemanticGraph,
        metric: Metric,
        metric_name_to_node: dict[str, SemanticGraphNode],
    ) -> None:
        metric_type = metric.type
        if metric_type is MetricType.DERIVED:
            self._add_elements_for_derived_metric(
                current_subgraph=current_subgraph, derived_metric=metric, metric_name_to_node=metric_name_to_node
            )
        elif (
            metric_type is MetricType.SIMPLE
            or metric_type is MetricType.RATIO
            or metric_type is MetricType.CUMULATIVE
            or metric_type is MetricType.CONVERSION
        ):
            self._add_elements_for_base_metric(
                current_subgraph=current_subgraph, base_metric=metric, metric_name_to_node=metric_name_to_node
            )
        else:
            assert_values_exhausted(metric_type)

    @override
    def generate_subgraph(self, current_graph: SemanticGraph) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()
        if self._verbose_debug_logs:
            logger.debug(LazyFormat("Starting with graph", current_graph=current_graph))

        for metric in self._manifest_object_lookup.get_metrics():
            self._add_elements_for_any_metric(
                current_subgraph=current_subgraph,
                metric=metric,
                metric_name_to_node={},
            )

            # metric_type = metric.type
            # if metric_type is MetricType.DERIVED:
            #     self._add_elements_for_derived_metric(current_subgraph=current_subgraph, derived_metric=metric)
            # elif (
            #     metric_type is MetricType.SIMPLE
            #     or metric_type is MetricType.RATIO
            #     or metric_type is MetricType.CUMULATIVE
            #     or metric_type is MetricType.CONVERSION
            # ):
            #     self._add_elements_for_base_metric(current_subgraph=current_subgraph, base_metric=metric)
            # else:
            #     assert_values_exhausted(metric_type)

        return current_subgraph

        # metric_depth_lookup = self._resolve_metric_depth_lookup()

        # for depth in range(max(metric_depth_lookup.depth_to_metrics.keys())):
        #     if depth == 0:
        #         required_measure_nodes = MutableOrderedSet[SemanticGraphNode]()
        #         for measure in metric.input_measures:
        #             measure_node = mf_first_item(
        #                 current_graph.nodes_with_label(MeasureLabel(measure_name=measure.name)))
        #             required_measure_nodes.add(measure_node)
        #
        #         for measure_node in required_measure_nodes:
        #             current_subgraph.add_edge(
        #                 MetricDefinitionEdge.get_instance(
        #                     tail_node=MetricNode.get_instance(metric.name), head_node=measure_node
        #                 )
        #             )

        # return current_subgraph


@dataclass
class _MetricDepthLookup:
    depth_to_metrics: dict[int, list[Metric]]
    metric_name_to_depth: dict[str, int]
