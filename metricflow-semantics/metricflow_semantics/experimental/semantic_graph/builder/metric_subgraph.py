from __future__ import annotations

import logging

from dbt_semantic_interfaces.protocols import Metric
from typing_extensions import override

from metricflow_semantics.collection_helpers.syntactic_sugar import mf_first_item
from metricflow_semantics.experimental.ordered_set import MutableOrderedSet
from metricflow_semantics.experimental.semantic_graph.builder.graph_change_rule import (
    SemanticSubgraphGenerator,
    SubgraphGeneratorArgumentSet,
)
from metricflow_semantics.experimental.semantic_graph.edges.entity_relationship import MetricDefinitionEdge
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import (
    MetricNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    MeasureAttributeLabel,
    MetricAttributeLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph, SemanticGraph
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


class MetricSubgraphGenerator(SemanticSubgraphGenerator):
    def __init__(self, argument_set: SubgraphGeneratorArgumentSet) -> None:
        super().__init__(argument_set)
        self._verbose_debug_logs = False

    def _generate_subgraph_for_any_metric(
        self, current_graph: SemanticGraph, current_subgraph: MutableSemanticGraph, metric: Metric
    ) -> None:
        if len(current_subgraph.nodes_with_label(MetricAttributeLabel(metric_name=metric.name))) > 0:
            return

        parent_metric_inputs = metric.type_params.metrics
        if parent_metric_inputs is None:
            self._generate_subgraph_for_base_metric(current_graph, current_subgraph, metric)
            return

        for parent_metric_input in parent_metric_inputs:
            parent_metric = self._manifest_object_lookup.get_metric(parent_metric_input.name)
            if parent_metric.type_params.metrics is None:
                self._generate_subgraph_for_base_metric(current_graph, current_subgraph, parent_metric)
            else:
                self._generate_subgraph_for_any_metric(current_graph, current_subgraph, parent_metric)
            current_subgraph.add_edge(
                MetricDefinitionEdge.get_instance(
                    tail_node=MetricNode(attribute_name=metric.name),
                    head_node=MetricNode(attribute_name=parent_metric_input.name),
                )
            )

    def _generate_subgraph_for_base_metric(
        self, current_graph: SemanticGraph, current_subgraph: MutableSemanticGraph, metric: Metric
    ) -> None:
        if len(current_subgraph.nodes_with_label(MetricAttributeLabel(metric_name=metric.name))) > 0:
            return
        required_measure_nodes = MutableOrderedSet[SemanticGraphNode]()
        for measure in metric.input_measures:
            measure_node = mf_first_item(
                current_graph.nodes_with_label(MeasureAttributeLabel(measure_name=measure.name))
            )
            required_measure_nodes.add(measure_node)

        for measure_node in required_measure_nodes:
            current_subgraph.add_edge(
                MetricDefinitionEdge.get_instance(
                    tail_node=MetricNode(attribute_name=metric.name), head_node=measure_node
                )
            )

    @override
    def generate_subgraph(self, current_graph: SemanticGraph) -> MutableSemanticGraph:
        current_subgraph = MutableSemanticGraph.create()
        if self._verbose_debug_logs:
            logger.debug(LazyFormat("Starting with graph", current_graph=current_graph))
        for metric in self._manifest_object_lookup.get_metrics():
            self._generate_subgraph_for_any_metric(current_graph, current_subgraph, metric)

        return current_subgraph
