from __future__ import annotations

import logging

from dbt_semantic_interfaces.protocols import Metric
from typing_extensions import override

from metricflow_semantics.collection_helpers.syntactic_sugar import mf_first_item
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import AttributeComputationUpdate
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeComputationPath,
)
from metricflow_semantics.experimental.semantic_graph.builder.dunder_name_weight import DunderNameWeightFunction
from metricflow_semantics.experimental.semantic_graph.builder.graph_change_rule import (
    SemanticSubgraphGenerator,
    SubgraphGeneratorArgumentSet,
)
from metricflow_semantics.experimental.semantic_graph.edges.entity_attribute import (
    AttributeEdgeType,
    EntityAttributeEdge,
)
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import (
    MetricNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    DsiEntityLabel,
    MeasureAttributeLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph, SemanticGraph
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType

logger = logging.getLogger(__name__)


class GroupByMetricSubgraph(SemanticSubgraphGenerator):
    def __init__(self, argument_set: SubgraphGeneratorArgumentSet) -> None:
        super().__init__(argument_set)
        self._mutable_path = AttributeComputationPath.create()
        self._verbose_debug_logs = False
        self._metric_name_to_connected_dsi_entity_nodes: dict[str, FrozenOrderedSet[SemanticGraphNode]] = {}
        self._metric_name_to_derivative_semantic_model_ids: dict[str, FrozenOrderedSet[SemanticModelId]] = {}

    def _metric_processed(self, metric_name: str) -> bool:
        """Returns true if this metric has been processed and recursive calls don't need to be made on the parents.

        This check is called before the generate method is called to reduce the number of recursive calls that show up
        in profiling results.
        """
        # return len(current_subgraph.nodes_with_label(MetricAttributeLabel(metric_name=metric_name))) > 0
        return metric_name in self._metric_name_to_connected_dsi_entity_nodes

    def _generate_subgraph_for_any_metric(
        self, current_graph: SemanticGraph, subgraph: MutableSemanticGraph, metric: Metric
    ) -> None:
        metric_name = metric.name

        if self._metric_processed(metric_name):
            return

        parent_metric_inputs = metric.type_params.metrics
        if parent_metric_inputs is None:
            self._generate_subgraph_for_base_metric(current_graph, subgraph, metric)
            return

        reachable_dsi_entity_nodes = FrozenOrderedSet[SemanticGraphNode]()
        derivative_semantic_model_ids = FrozenOrderedSet[SemanticModelId]()

        for i, parent_metric_input in enumerate(parent_metric_inputs):
            parent_metric = self._manifest_object_lookup.get_metric(parent_metric_input.name)
            parent_metric_name = parent_metric.name
            if not self._metric_processed(parent_metric_name):
                if parent_metric.type_params.metrics is None:
                    self._generate_subgraph_for_base_metric(current_graph, subgraph, parent_metric)
                else:
                    self._generate_subgraph_for_any_metric(current_graph, subgraph, parent_metric)

            if self._metric_processed(metric_name):
                return

            reachable_dsi_entity_nodes_for_parent_metric = self._metric_name_to_connected_dsi_entity_nodes[
                parent_metric_name
            ]
            derivative_semantic_model_ids_for_parent_metric = self._metric_name_to_derivative_semantic_model_ids[
                parent_metric_name
            ]
            if i == 0:
                reachable_dsi_entity_nodes = reachable_dsi_entity_nodes_for_parent_metric
                derivative_semantic_model_ids = derivative_semantic_model_ids_for_parent_metric
            else:
                reachable_dsi_entity_nodes = reachable_dsi_entity_nodes.intersection(
                    reachable_dsi_entity_nodes_for_parent_metric
                )
                derivative_semantic_model_ids = derivative_semantic_model_ids.union(
                    derivative_semantic_model_ids_for_parent_metric
                )

        metric_node = MetricNode.get_instance(metric_name)
        for reachable_dsi_entity_node in reachable_dsi_entity_nodes:
            subgraph.add_edge(
                EntityAttributeEdge.get_instance(
                    tail_node=reachable_dsi_entity_node,
                    head_node=metric_node,
                    attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
                    attribute_computation_update=AttributeComputationUpdate(
                        dundered_name_element_addition=metric_name,
                        derived_from_model_id_additions=tuple(derivative_semantic_model_ids),
                        linkable_element_property_additions=(LinkableElementProperty.METRIC,),
                        element_type_addition=LinkableElementType.METRIC,
                    ),
                )
            )

        self._metric_name_to_connected_dsi_entity_nodes[metric_name] = reachable_dsi_entity_nodes
        self._metric_name_to_derivative_semantic_model_ids[metric_name] = derivative_semantic_model_ids

    def _generate_subgraph_for_base_metric(
        self, current_graph: SemanticGraph, subgraph: MutableSemanticGraph, metric: Metric
    ) -> None:
        metric_name = metric.name
        if self._metric_processed(metric_name):
            return

        required_measure_nodes = MutableOrderedSet[SemanticGraphNode]()
        semantic_model_ids = MutableOrderedSet[SemanticModelId]()
        for measure in metric.input_measures:
            measure_node = mf_first_item(
                current_graph.nodes_with_label(MeasureAttributeLabel(measure_name=measure.name))
            )
            required_measure_nodes.add(measure_node)
            semantic_model_ids.update(measure_node.attribute_computation_update.derived_from_model_id_additions)

        source_nodes = required_measure_nodes.as_frozen()
        candidate_target_nodes = current_graph.nodes_with_label(DsiEntityLabel()).as_frozen()
        common_reachable_targets_result = self._path_finder.find_common_reachable_targets(
            graph=current_graph,
            mutable_path=self._mutable_path,
            source_nodes=source_nodes,
            candidate_target_nodes=candidate_target_nodes,
            weight_function=DunderNameWeightFunction(),
            max_path_weight=1,
        )

        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Completed search for reachable targets",
                    reachable_target_count=len(common_reachable_targets_result.reachable_targets),
                    result=common_reachable_targets_result,
                )
            )

        metric_attribute_node = MetricNode(attribute_name=metric_name)
        for reachable_dsi_entity_node in common_reachable_targets_result.reachable_targets:
            subgraph.add_edge(
                EntityAttributeEdge.get_instance(
                    tail_node=reachable_dsi_entity_node,
                    head_node=metric_attribute_node,
                    attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
                    attribute_computation_update=AttributeComputationUpdate(
                        dundered_name_element_addition=metric_name,
                        derived_from_model_id_additions=tuple(semantic_model_ids),
                        linkable_element_property_additions=(LinkableElementProperty.METRIC,),
                        element_type_addition=LinkableElementType.METRIC,
                    ),
                )
            )

        self._metric_name_to_connected_dsi_entity_nodes[
            metric_name
        ] = common_reachable_targets_result.reachable_targets.as_frozen()
        self._metric_name_to_derivative_semantic_model_ids[metric_name] = semantic_model_ids.as_frozen()

    @override
    def generate_subgraph(self, current_graph: SemanticGraph) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()
        if self._verbose_debug_logs:
            logger.debug(LazyFormat("Starting with graph", current_graph=current_graph))
        for metric in self._manifest_object_lookup.get_metrics():
            if not self._metric_processed(metric.name):
                self._generate_subgraph_for_any_metric(current_graph, current_subgraph, metric)

        return current_subgraph
