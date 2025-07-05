from __future__ import annotations

import logging

from dbt_semantic_interfaces.protocols import Metric
from typing_extensions import override

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import AttributeRecipeUpdate
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.experimental.semantic_graph.builder.graph_change_rule import (
    SemanticSubgraphGenerator,
    SubgraphGeneratorArgumentSet,
)
from metricflow_semantics.experimental.semantic_graph.edges.entity_relationship import EntityRelationshipEdge
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import (
    MetricAttributeNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import (
    KeyEntityNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    DsiEntityLabel,
    LocalModelLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph, SemanticGraph
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


class GroupByMetricSubgraph(SemanticSubgraphGenerator):
    def __init__(self, argument_set: SubgraphGeneratorArgumentSet) -> None:
        super().__init__(argument_set)
        self._mutable_path = AttributeRecipeWriterPath.create()
        self._verbose_debug_logs = False
        self._metric_name_to_connected_dsi_entity_nodes: dict[str, FrozenOrderedSet[SemanticGraphNode]] = {}
        self._metric_name_to_derivative_semantic_model_ids: dict[str, FrozenOrderedSet[SemanticModelId]] = {}

        self._current_graph: SemanticGraph = MutableSemanticGraph.create()
        self._traversable_nodes_for_dsi_entity_node_search: OrderedSet[SemanticGraphNode] = FrozenOrderedSet()
        self._measure_name_to_edge_argument: dict[str, MetricAttributeEdgeArgument] = {}
        self._metric_name_to_edge_argument: dict[str, MetricAttributeEdgeArgument] = {}

    # def _metric_processed(self, metric_name: str) -> bool:
    #     """Returns true if this metric has been processed and recursive calls don't need to be made on the parents.
    #
    #     This check is called before the generate method is called to reduce the number of recursive calls that show up
    #     in profiling results.
    #     """
    #     # return len(current_subgraph.nodes_with_label(MetricAttributeLabel(metric_name=metric_name))) > 0
    #     return metric_name in self._metric_name_to_connected_dsi_entity_nodes

    def _generate_subgraph_for_any_metric(self, subgraph: MutableSemanticGraph, metric: Metric) -> None:
        metric_name = metric.name

        if metric_name in self._metric_name_to_edge_argument:
            return

        # if self._metric_processed(metric_name):
        #     return

        parent_metric_inputs = metric.type_params.metrics
        if parent_metric_inputs is None:
            self._generate_subgraph_for_base_metric(subgraph, metric)
            return

        assert len(parent_metric_inputs) > 0

        for parent_metric_input in parent_metric_inputs:
            self._generate_subgraph_for_any_metric(
                subgraph, self._manifest_object_lookup.get_metric(parent_metric_input.name)
            )

        first_parent_metric_input = parent_metric_inputs[0]
        edge_argument = self._metric_name_to_edge_argument[first_parent_metric_input.name]

        for other_parent_metric_input in parent_metric_inputs[1:]:
            edge_argument = edge_argument.intersection(
                self._metric_name_to_edge_argument[other_parent_metric_input.name]
            )

        # reachable_dsi_entity_nodes = FrozenOrderedSet[SemanticGraphNode]()
        # source_model_ids = FrozenOrderedSet[SemanticModelId]()
        #
        # for i, parent_metric_input in enumerate(parent_metric_inputs):
        #     parent_metric = self._manifest_object_lookup.get_metric(parent_metric_input.name)
        #     parent_metric_name = parent_metric.name
        #     if not self._metric_processed(parent_metric_name):
        #         if parent_metric.type_params.metrics is None:
        #             self._generate_subgraph_for_base_metric(subgraph, parent_metric)
        #         else:
        #             self._generate_subgraph_for_any_metric(subgraph, parent_metric)
        #
        #     if self._metric_processed(metric_name):
        #         return
        #
        #     reachable_dsi_entity_nodes_for_parent_metric = self._metric_name_to_connected_dsi_entity_nodes[
        #         parent_metric_name
        #     ]
        #     source_model_ids_for_parent_metric = self._metric_name_to_derivative_semantic_model_ids[parent_metric_name]
        #     if i == 0:
        #         reachable_dsi_entity_nodes = reachable_dsi_entity_nodes_for_parent_metric
        #         source_model_ids = source_model_ids_for_parent_metric
        #     else:
        #         reachable_dsi_entity_nodes = reachable_dsi_entity_nodes.intersection(
        #             reachable_dsi_entity_nodes_for_parent_metric
        #         )
        #         source_model_ids = source_model_ids.union(source_model_ids_for_parent_metric)
        # attribute_computation_update = AttributeComputationUpdate(
        #     # dundered_name_element_addition=metric_name,
        #     source_model_id_additions=tuple(source_model_ids),
        #     # linkable_element_property_additions=(LinkableElementProperty.METRIC,),
        #     # element_type_addition=LinkableElementType.METRIC,
        # )
        # metric_node = MetricAttributeNode.get_instance(metric_name)
        # for reachable_dsi_entity_node in reachable_dsi_entity_nodes:
        #     subgraph.add_edge(
        #         EntityAttributeEdge.get_instance(
        #             tail_node=reachable_dsi_entity_node,
        #             head_node=metric_node,
        #             attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
        #             attribute_computation_update=attribute_computation_update,
        #         )
        #     )
        #
        # self._metric_name_to_connected_dsi_entity_nodes[metric_name] = reachable_dsi_entity_nodes
        # self._metric_name_to_derivative_semantic_model_ids[metric_name] = source_model_ids

    def _get_edge_argument_for_measure(
        self,
        measure_name: str,
    ) -> MetricAttributeEdgeArgument:
        edge_argument = self._measure_name_to_edge_argument.get(measure_name)

        if edge_argument is not None:
            return edge_argument

        semantic_model = self._manifest_object_lookup.get_semantic_model_containing_measure(measure_name)

        # primary_entity_reference = semantic_model.primary_entity_reference
        #
        # possible_entity_links = []
        # if primary_entity_reference is not None:
        #     possible_entity_links.append(primary_entity_reference.element_name)
        #
        # for entity in semantic_model.entities:
        #     if entity.is_linkable_entity_type:
        #         possible_entity_links.append(entity.reference)
        #
        # valid_subquery_entity_link_tuples = []
        # entity_attribute_names = []
        # for entity in semantic_model.entities:
        #     for possible_entity_link in possible_entity_links:
        #         valid_subquery_entity_link_tuples.append((possible_entity_link, entity.name))
        #     entity_attribute_names.append(entity.name)

        edge_argument = MetricAttributeEdgeArgument(
            model_ids=FrozenOrderedSet((SemanticModelId.get_instance(semantic_model.name),)),
            # metric_subqueries=FrozenOrderedSet(valid_subquery_entity_link_tuples),
            predecessor_nodes=FrozenOrderedSet(
                KeyEntityNode.get_instance(entity.name) for entity in semantic_model.entities
            ),
        )
        self._measure_name_to_edge_argument[measure_name] = edge_argument
        return edge_argument
        #
        # first_model_id = model_ids[0]
        # other_model_ids = model_ids[1:]
        #
        # return self._path_finder.find_reachable_targets(
        #     graph=self._current_graph,
        #     source_nodes=FrozenOrderedSet((SemanticModelNode.get_instance(model_id),)),
        #     candidate_target_nodes=self._current_graph.nodes_with_label(DsiEntityLabel.get_instance()),
        #     traversable_nodes=self._traversable_nodes_for_dsi_entity_node_search,
        # )

    def _generate_subgraph_for_base_metric(self, subgraph: MutableSemanticGraph, metric: Metric) -> None:
        metric_name = metric.name
        edge_argument = self._metric_name_to_edge_argument.get(metric_name)

        if edge_argument is not None:
            return

        input_measures = metric.input_measures
        assert len(input_measures) > 0

        edge_argument = self._get_edge_argument_for_measure(input_measures[0].name)

        for input_measure in input_measures[1:]:
            edge_argument = edge_argument.intersection(self._get_edge_argument_for_measure(input_measure.name))

        metric_attribute_node = MetricAttributeNode.get_instance(metric_name)

        for tail_node in edge_argument.predecessor_nodes:
            subgraph.add_edge(
                EntityRelationshipEdge.get_instance(
                    tail_node=tail_node,
                    head_node=metric_attribute_node,
                    attribute_computation_update=AttributeRecipeUpdate(
                        add_entity_link=tail_node.dsi_entity_name,
                        add_dunder_name_element=tail_node.dsi_entity_name,
                        add_subquery_model_ids=tuple(edge_argument.model_ids),
                    ),
                )
            )

        self._metric_name_to_edge_argument[metric_name] = edge_argument
        return

        # required_measure_nodes = MutableOrderedSet[SemanticGraphNode]()
        # semantic_model_ids = MutableOrderedSet[SemanticModelId]()
        #
        # for measure in metric.input_measures:
        #     model_id = self._manifest_object_lookup.measure_name_to_model_id[measure.name]
        #     measure_node = MeasureNode.get_instance(measure_name=measure.name, model_id=model_id)
        #     semantic_model_ids.add(model_id)
        #     required_measure_nodes.add(measure_node)
        #
        # source_nodes = required_measure_nodes.as_frozen()
        # candidate_target_nodes = current_graph.nodes_with_label(DsiEntityLabel()).as_frozen()
        # common_reachable_targets_result = self._path_finder.find_common_reachable_targets(
        #     graph=current_graph,
        #     mutable_path=self._mutable_path,
        #     source_nodes=source_nodes,
        #     candidate_target_nodes=candidate_target_nodes,
        #     weight_function=DunderNameWeightFunction(),
        #     max_path_weight=1,
        # )

        # for stop_event in self._path_finder.traverse_dfs(
        #     graph=current_graph,
        #     mutable_path=self._mutable_path,
        #     source_node=source_nodes,
        #     target_nodes=target_nodes,
        #     weight_function=DunderNameWeightFunction(),
        #     max_path_weight=DunderNameWeightFunction.MAX_ENTITY_LINKS,
        #     allow_node_revisits=True,
        # ):
        #     path = stop_event.current_path
        #     attribute_descriptor = mutable_path.attribute_computation.attribute_descriptor

        # if self._verbose_debug_logs:
        #     logger.debug(
        #         LazyFormat(
        #             "Completed search for reachable targets",
        #             reachable_target_count=len(common_reachable_targets_result.reachable_targets),
        #             result=common_reachable_targets_result,
        #         )
        #     )
        # attribute_computation_update = AttributeComputationUpdate(
        #     # dundered_name_element_addition=metric_name,
        #     source_model_id_additions=tuple(semantic_model_ids),
        #     # linkable_element_property_additions=(LinkableElementProperty.METRIC,),
        #     # element_type_addition=LinkableElementType.METRIC,
        # )
        # metric_attribute_node = MetricAttributeNode.get_instance(metric_name)
        # for reachable_dsi_entity_node in common_reachable_targets_result.reachable_targets:
        #     subgraph.add_edge(
        #         EntityAttributeEdge.get_instance(
        #             tail_node=reachable_dsi_entity_node,
        #             head_node=metric_attribute_node,
        #             attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
        #             attribute_computation_update=attribute_computation_update,
        #         )
        #     )
        #
        # self._metric_name_to_connected_dsi_entity_nodes[
        #     metric_name
        # ] = common_reachable_targets_result.reachable_targets.as_frozen()
        # self._metric_name_to_derivative_semantic_model_ids[metric_name] = semantic_model_ids.as_frozen()

    @override
    def generate_subgraph(self, current_graph: SemanticGraph) -> MutableSemanticGraph:
        self._current_graph = current_graph
        self._traversable_nodes_for_dsi_entity_node_search = current_graph.nodes_with_label(
            LocalModelLabel.get_instance()
        ).union(current_graph.nodes_with_label(DsiEntityLabel.get_instance()))
        self._measure_name_to_edge_argument.clear()

        current_subgraph = MutableSemanticGraph.create()
        if self._verbose_debug_logs:
            logger.debug(LazyFormat("Starting with graph", current_graph=current_graph))
        for metric in self._manifest_object_lookup.get_metrics():
            if metric.name not in self._metric_name_to_edge_argument:
                self._generate_subgraph_for_any_metric(current_subgraph, metric)

        return current_subgraph


@fast_frozen_dataclass()
class MetricAttributeEdgeArgument:
    model_ids: FrozenOrderedSet[SemanticModelId]
    # metric_subqueries: FrozenOrderedSet[AnyLengthTuple]
    predecessor_nodes: FrozenOrderedSet[KeyEntityNode]

    def intersection(self, other: MetricAttributeEdgeArgument) -> MetricAttributeEdgeArgument:
        return MetricAttributeEdgeArgument(
            model_ids=self.model_ids.union(other.model_ids),
            # metric_subqueries=FrozenOrderedSet(
            #     self.metric_subqueries.intersection(other.metric_subqueries)
            # ),
            predecessor_nodes=FrozenOrderedSet(self.predecessor_nodes.intersection(other.predecessor_nodes)),
        )
