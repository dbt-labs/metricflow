from __future__ import annotations

import logging
from collections.abc import Set
from dataclasses import dataclass
from functools import cached_property

from typing_extensions import override

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import AttributeRecipeUpdate
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_resolver import (
    AttributeResolver,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.key_query_set import DsiEntityKeyQuerySet
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
    GroupByMetricNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import (
    KeyEntityNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    DsiEntityLabel,
    LocalModelLabel,
    MeasureLabel,
    MetricLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder import MetricflowGraphPathFinder
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph, SemanticGraph
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)

# @dataclass
# class MetricSubquery:
#     metric_name: str
#     entity_paths: AnyLengthTuple[AnyLengthTuple[str]]
#
# EntityPath = AnyLengthTuple[str]
#
#
# @fast_frozen_dataclass
# class EntityKeyQueryCandidate:
#     key_entity_node: KeyEntityNode
#     entity_key_queries: OrderedSet[EntityKeyQuery]
#
#
# @fast_frozen_dataclass
# class EntityKeyQueryCandidateSet:
#     candidates: AnyLengthTuple[EntityKeyQueryCandidate]
#
#     @cached_property
#     def key_entity_nodes(self) -> FrozenOrderedSet[KeyEntityNode]:
#         return FrozenOrderedSet(candidate.key_entity_node for candidate in self.candidates)
#
#     @staticmethod
#     def intersection(candidate_sets: Sequence[EntityKeyQueryCandidateSet]) -> EntityKeyQueryCandidateSet:
#         if len(candidate_sets) == 0:
#             return EntityKeyQueryCandidateSet(candidates=())
#
#         if len(candidate_sets) == 1:
#             return candidate_sets[0]
#
#         common_key_entity_nodes = set.intersection(
#             *(candidate_set.key_entity_nodes for candidate_set in candidate_sets)
#         )
#
#         if len(common_key_entity_nodes) == 0:
#             return EntityKeyQueryCandidateSet(candidates=())
#
#         key_entity_node_to_query_intersection: dict[
#             KeyEntityNode, OrderedIntersection[EntityKeyQueryCandidate]
#         ] = defaultdict(OrderedIntersection)
#
#         for candidate_set in candidate_sets:
#             for candidate in candidate_set.candidates:
#                 key_entity_node = candidate.key_entity_node
#                 if key_entity_node not in common_key_entity_nodes:
#                     pass
#                 key_entity_node_to_query_intersection[key_entity_node].intersect(candidate.entity_key_queries)
#
#         return EntityKeyQueryCandidateSet(
#             candidates=tuple(key_entity_node_to_query_intersection)
#         )
#
#         raise NotImplementedError


@dataclass
class _GenerateGroupByMetricSubgraphContext:
    def __init__(
        self,
        current_graph: SemanticGraph,
        attribute_resolver: AttributeResolver,
        path_finder: MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath],
    ) -> None:
        self._current_graph = current_graph
        self._attribute_resolver = attribute_resolver
        self._path_finder = path_finder
        self._metric_node_to_key_query_set: dict[SemanticGraphNode, DsiEntityKeyQuerySet] = {}
        metric_nodes = current_graph.nodes_with_label(MetricLabel.get_instance())
        self._metric_nodes = metric_nodes
        self._verbose_debug_logs = True

    # def get_key_entity_node_to_entity_key_queries(
    #     self, local_model_node: SemanticGraphNode
    # ) -> dict[KeyEntityNode, OrderedSet[EntityKeyQuery]]:
    #     entity_key_queries = self._attribute_resolver.generate_entity_key_queries(local_model_node)
    #
    #     key_entity_node_to_entity_key_queries: DefaultDict[
    #         KeyEntityNode, MutableOrderedSet[EntityKeyQuery]
    #     ] = defaultdict(MutableOrderedSet)
    #
    #     for entity_key_query in entity_key_queries:
    #         key_entity_node = KeyEntityNode.get_instance(entity_key_query[-1])
    #         key_entity_node_to_entity_key_queries[key_entity_node].add(entity_key_query)
    #
    #     return key_entity_node_to_entity_key_queries

    @cached_property
    def _model_node_to_key_query_set(self) -> dict[SemanticGraphNode, DsiEntityKeyQuerySet]:
        current_graph = self._current_graph
        local_model_nodes = current_graph.nodes_with_label(LocalModelLabel.get_instance())
        result = {}

        for local_model_node in local_model_nodes:
            recipe_update = local_model_node.attribute_recipe_update
            model_id = recipe_update.join_model
            if model_id is None:
                raise RuntimeError(
                    LazyFormat(
                        "Expected a local model node to have an associated model ID",
                        local_model_node=local_model_node,
                        recipe_update=recipe_update,
                    )
                )
            result[local_model_node] = DsiEntityKeyQuerySet(
                source_model_ids=FrozenOrderedSet((model_id,)),
                entity_key_queries=FrozenOrderedSet(
                    self._attribute_resolver.generate_entity_key_queries(local_model_node)
                ),
            )
        return result

    def _get_key_query_set_for_metric_node(
        self,
        metric_node: SemanticGraphNode,
        _metric_nodes_in_definition_path: Set[SemanticGraphNode] = frozenset(),
    ) -> DsiEntityKeyQuerySet:
        entity_key_query_set = self._metric_node_to_key_query_set.get(metric_node)
        if entity_key_query_set is not None:
            return entity_key_query_set

        if metric_node in _metric_nodes_in_definition_path:
            raise RuntimeError(
                LazyFormat(
                    "Recursive metric definition detected",
                    metric_node=metric_node,
                    metric_nodes_in_definition_path=_metric_nodes_in_definition_path,
                )
            )

        if metric_node not in self._metric_nodes:
            raise RuntimeError(
                LazyFormat(
                    "Traversal reached a non-metric node",
                    current_node=metric_node,
                    metric_nodes=self._metric_nodes,
                )
            )

        current_graph = self._current_graph
        parent_metric_nodes = current_graph.predecessors(metric_node)

        key_query_sets_to_intersect = []
        for parent_metric_node in parent_metric_nodes:
            if parent_metric_node not in self._metric_node_to_key_query_set:
                self._get_key_query_set_for_metric_node(
                    metric_node=parent_metric_node,
                    _metric_nodes_in_definition_path=_metric_nodes_in_definition_path | {metric_node},
                )
            key_query_sets_to_intersect.append(self._metric_node_to_key_query_set[parent_metric_node])
        intersected_key_query_set = DsiEntityKeyQuerySet.intersection(key_query_sets_to_intersect)
        self._metric_node_to_key_query_set[metric_node] = intersected_key_query_set

        return intersected_key_query_set

    def generate(self) -> MutableSemanticGraph:
        current_graph = self._current_graph

        local_model_nodes = current_graph.nodes_with_label(LocalModelLabel.get_instance())
        # local_model_node_to_key_queries = {}
        # for local_model_node in local_model_nodes:
        #     self.get_metric_subquery(local_model_node)
        inverse_current_graph = self._current_graph.inverse()
        measure_nodes = current_graph.nodes_with_label(MeasureLabel.get_instance())
        local_model_nodes = current_graph.nodes_with_label(LocalModelLabel.get_instance())

        path_finder = self._path_finder
        allowed_nodes_for_walking_between_measures_and_metrics = measure_nodes.union(self._metric_nodes)
        base_metric_nodes = path_finder.find_reachable_targets(
            graph=inverse_current_graph,
            source_nodes=measure_nodes,
            candidate_target_nodes=self._metric_nodes,
            allowed_nodes=allowed_nodes_for_walking_between_measures_and_metrics,
        )
        if len(base_metric_nodes) == 0:
            raise RuntimeError(
                LazyFormat(
                    "Did not find any base metric nodes. This indicates an error in graph construction.",
                    measure_nodes=measure_nodes,
                    metric_nodes=self._metric_nodes,
                    allowed_nodes=allowed_nodes_for_walking_between_measures_and_metrics,
                )
            )

        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Found base-metric nodes",
                    base_metric_nodes=base_metric_nodes,
                )
            )

        self._metric_node_to_key_query_set.clear()

        # allowed_nodes_for_walking_from_metric_nodes_to_local_model_nodes =
        allowed_nodes_for_walking_from_metrics_to_models = allowed_nodes_for_walking_between_measures_and_metrics.union(
            local_model_nodes
        )
        for base_metric_node in base_metric_nodes:
            measure_model_nodes = path_finder.find_reachable_targets(
                graph=current_graph,
                source_nodes=FrozenOrderedSet((base_metric_node,)),
                candidate_target_nodes=local_model_nodes,
                allowed_nodes=allowed_nodes_for_walking_from_metrics_to_models,
            )

            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Found model nodes that correspond to the measures for the metric",
                        base_metric_node=base_metric_node,
                        measure_model_nodes=measure_model_nodes,
                    )
                )

            if len(measure_model_nodes) == 0:
                raise RuntimeError(
                    LazyFormat(
                        "Did not find any model nodes for the given metric node.",
                        base_metric_node=base_metric_node,
                        measure_model_nodes=measure_model_nodes,
                        allowed_nodes=allowed_nodes_for_walking_from_metrics_to_models,
                    )
                )

            key_query_sets_to_intersect: list[DsiEntityKeyQuerySet] = []
            for measure_model_node in measure_model_nodes:
                key_query_sets_to_intersect.append(self._model_node_to_key_query_set[measure_model_node])

            self._metric_node_to_key_query_set[base_metric_node] = DsiEntityKeyQuerySet.intersection(
                key_query_sets_to_intersect
            )

        metric_name_to_key_queries: dict[str, DsiEntityKeyQuerySet] = {}
        subgraph = MutableSemanticGraph.create()

        for metric_node in self._metric_nodes:
            key_query_set = self._get_key_query_set_for_metric_node(metric_node)
            metric_name = metric_node.attribute_recipe_update.add_dunder_name_element
            if metric_name is None:
                raise RuntimeError(
                    LazyFormat(
                        "Expected a metric node to have a name",
                        metric_node=metric_node,
                        recipe_update=metric_node.attribute_recipe_update,
                    )
                )
            metric_name_to_key_queries[metric_name] = key_query_set

            group_by_metric_node = GroupByMetricNode.get_instance(metric_name)

            key_names = FrozenOrderedSet(entity_key_query[-1] for entity_key_query in key_query_set.entity_key_queries)

            for key_name in key_names:
                key_entity_node = KeyEntityNode.get_instance(key_name)
                subgraph.add_edge(
                    EntityAttributeEdge.get_instance(
                        tail_node=key_entity_node,
                        attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
                        head_node=group_by_metric_node,
                        attribute_recipe_update=AttributeRecipeUpdate(
                            provide_key_query_set=key_query_set.filter_by_key_name(key_name),
                        ),
                    )
                )

        return subgraph

        # processed_metric_nodes = MutableOrderedSet(base_metric_nodes)
        #
        # metric_nodes_to_process = OrderedUnion.union_sets(
        #     current_graph.predecessors(base_metric_node)
        #     for base_metric_node in base_metric_nodes
        # )
        #
        # while len(metric_nodes_to_process) != 0:
        #
        #     metric_node_to_parent_metric_node: dict[SemanticGraphNode, MutableOrderedSet[SemanticGraphNode]] = defaultdict(MutableOrderedSet)
        #     for metric_node in metric_nodes_to_process:
        #         entity_key_queries_for_metric_node = metric_node_to_key_query_intersection[metric_node]
        #         for parent_metric_node in current_graph.predecessors(metric_node):
        #             metric_node_to_key_query_intersection[parent_metric_node].intersect(
        #                 entity_key_queries_for_metric_node.result()
        #             )
        #     for metric_node in metric_nodes_to_process:
        #         if metric_node in processed_metric_nodes:
        #             continue
        #
        #         parent_nodes = current_graph.predecessors(metric_node)
        #
        #         if len(parent_nodes.intersection())
        #         intersected_key_queries = OrderedIntersection.intersect_sets(
        #             self._model_node_to_key_queries[parent_node]
        #             for parent_node in parent_nodes
        #         )
        #         metric_node_to_key_query_intersection[metric_node] = intersected_key_queries
        #         processed_metric_nodes.add(metric_node)
        #
        #         next_set_of_metric_nodes_to_process.update(current_graph.predecessors(metric_node))
        #     metric_nodes_to_process = OrderedUnion.union_sets(
        #         current_graph.predecessors(base_metric_node)
        #         for base_metric_node in base_metric_nodes
        #     )

        # base_metric_nodes = MutableOrderedSet[SemanticGraphNode]()
        # derived_metric_nodes = MutableOrderedSet[SemanticGraphNode]()
        #
        # for metric_node in metric_nodes:
        #     for metric_node_to_successor_node_edge in current_graph.edges_with_tail_node(metric_node):
        #         if metric_node_to_successor_node_edge.head_node in measure_nodes:
        #             ba
        #
        # base_metric_nodes = self.path_finder.find_reachable_targets(
        #     graph=inverse_current_graph,
        #     source_nodes=measure_nodes,
        #     candidate_target_nodes=metric_nodes,
        #     traversable_nodes=measure_nodes.union(metric_nodes)
        # )
        #
        # required_local_model_nodes = self.path_finder.find_reachable_targets(
        #     graph=current_graph,
        #     source_nodes=b,
        #     candidate_target_nodes=metric_nodes,
        #     traversable_nodes=measure_nodes.union(metric_nodes)
        # )


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

        current_graph = MutableSemanticGraph.create()
        self._generate_call_context = _GenerateGroupByMetricSubgraphContext(
            current_graph=current_graph,
            path_finder=self._path_finder,
            attribute_resolver=AttributeResolver(self._manifest_object_lookup, current_graph, self._path_finder),
        )

    # def _metric_processed(self, metric_name: str) -> bool:
    #     """Returns true if this metric has been processed and recursive calls don't need to be made on the parents.
    #
    #     This check is called before the generate method is called to reduce the number of recursive calls that show up
    #     in profiling results.
    #     """
    #     # return len(current_subgraph.nodes_with_label(MetricAttributeLabel(metric_name=metric_name))) > 0
    #     return metric_name in self._metric_name_to_connected_dsi_entity_nodes

    # def _generate_subgraph_for_any_metric(self, subgraph: MutableSemanticGraph, metric: Metric) -> None:
    #     metric_name = metric.name
    #
    #     if metric_name in self._metric_name_to_edge_argument:
    #         return
    #
    #     # if self._metric_processed(metric_name):
    #     #     return
    #
    #     parent_metric_inputs = metric.type_params.metrics
    #     if parent_metric_inputs is None:
    #         self._generate_subgraph_for_base_metric(subgraph, metric)
    #         return
    #
    #     assert len(parent_metric_inputs) > 0
    #
    #     for parent_metric_input in parent_metric_inputs:
    #         self._generate_subgraph_for_any_metric(
    #             subgraph, self._manifest_object_lookup.get_metric(parent_metric_input.name)
    #         )
    #
    #     first_parent_metric_input = parent_metric_inputs[0]
    #     edge_argument = self._metric_name_to_edge_argument[first_parent_metric_input.name]
    #
    #     for other_parent_metric_input in parent_metric_inputs[1:]:
    #         edge_argument = edge_argument.intersection(
    #             self._metric_name_to_edge_argument[other_parent_metric_input.name]
    #         )
    #
    #     metric_attribute_node = GroupByMetricNode.get_instance(metric_name)
    #
    #     for tail_node in edge_argument.predecessor_nodes:
    #         subgraph.add_edge(
    #             EntityRelationshipEdge.get_instance(
    #                 tail_node=tail_node,
    #                 head_node=metric_attribute_node,
    #                 recipe_update=AttributeRecipeUpdate(
    #                     add_entity_link=tail_node.dsi_entity_name,
    #                     add_dunder_name_element=tail_node.dsi_entity_name,
    #                     provide_key_query_set=tuple(edge_argument.model_ids),
    #                 ),
    #             )
    #         )
    #
    #     self._metric_name_to_edge_argument[metric_name] = edge_argument

    # def _get_edge_argument_for_measure(
    #     self,
    #     measure_name: str,
    # ) -> MetricAttributeEdgeArgument:
    #     edge_argument = self._measure_name_to_edge_argument.get(measure_name)
    #
    #     if edge_argument is not None:
    #         return edge_argument
    #
    #     semantic_model = self._manifest_object_lookup.get_semantic_model_containing_measure(measure_name)
    #
    #     # primary_entity_reference = semantic_model.primary_entity_reference
    #     #
    #     # possible_entity_links = []
    #     # if primary_entity_reference is not None:
    #     #     possible_entity_links.append(primary_entity_reference.element_name)
    #     #
    #     # for entity in semantic_model.entities:
    #     #     if entity.is_linkable_entity_type:
    #     #         possible_entity_links.append(entity.reference)
    #     #
    #     # valid_subquery_entity_link_tuples = []
    #     # entity_attribute_names = []
    #     # for entity in semantic_model.entities:
    #     #     for possible_entity_link in possible_entity_links:
    #     #         valid_subquery_entity_link_tuples.append((possible_entity_link, entity.name))
    #     #     entity_attribute_names.append(entity.name)
    #
    #     edge_argument = MetricAttributeEdgeArgument(
    #         model_ids=FrozenOrderedSet((SemanticModelId.get_instance(semantic_model.name),)),
    #         # metric_subqueries=FrozenOrderedSet(valid_subquery_entity_link_tuples),
    #         predecessor_nodes=FrozenOrderedSet(
    #             KeyEntityNode.get_instance(entity.name) for entity in semantic_model.entities
    #         ),
    #     )
    #     self._measure_name_to_edge_argument[measure_name] = edge_argument
    #     return edge_argument
    #     #
    #     # first_model_id = model_ids[0]
    #     # other_model_ids = model_ids[1:]
    #     #
    #     # return self._path_finder.find_reachable_targets(
    #     #     graph=self._current_graph,
    #     #     source_nodes=FrozenOrderedSet((SemanticModelNode.get_instance(model_id),)),
    #     #     candidate_target_nodes=self._current_graph.nodes_with_label(DsiEntityLabel.get_instance()),
    #     #     traversable_nodes=self._traversable_nodes_for_dsi_entity_node_search,
    #     # )
    #
    # def _generate_subgraph_for_base_metric(self, subgraph: MutableSemanticGraph, metric: Metric) -> None:
    #     metric_name = metric.name
    #     edge_argument = self._metric_name_to_edge_argument.get(metric_name)
    #
    #     if edge_argument is not None:
    #         return
    #
    #     input_measures = metric.input_measures
    #     assert len(input_measures) > 0
    #
    #     edge_argument = self._get_edge_argument_for_measure(input_measures[0].name)
    #
    #     for input_measure in input_measures[1:]:
    #         edge_argument = edge_argument.intersection(self._get_edge_argument_for_measure(input_measure.name))
    #
    #     metric_attribute_node = GroupByMetricNode.get_instance(metric_name)
    #
    #     for tail_node in edge_argument.predecessor_nodes:
    #         subgraph.add_edge(
    #             EntityRelationshipEdge.get_instance(
    #                 tail_node=tail_node,
    #                 head_node=metric_attribute_node,
    #                 recipe_update=AttributeRecipeUpdate(
    #                     add_entity_link=tail_node.dsi_entity_name,
    #                     add_dunder_name_element=tail_node.dsi_entity_name,
    #                     provide_key_query_set=tuple(edge_argument.model_ids),
    #                 ),
    #             )
    #         )
    #
    #     self._metric_name_to_edge_argument[metric_name] = edge_argument
    #     return
    #
    #     # required_measure_nodes = MutableOrderedSet[SemanticGraphNode]()
    #     # semantic_model_ids = MutableOrderedSet[SemanticModelId]()
    #     #
    #     # for measure in metric.input_measures:
    #     #     model_id = self._manifest_object_lookup.measure_name_to_model_id[measure.name]
    #     #     measure_node = MeasureNode.get_instance(measure_name=measure.name, model_id=model_id)
    #     #     semantic_model_ids.add(model_id)
    #     #     required_measure_nodes.add(measure_node)
    #     #
    #     # source_nodes = required_measure_nodes.as_frozen()
    #     # candidate_target_nodes = current_graph.nodes_with_label(DsiEntityLabel()).as_frozen()
    #     # common_reachable_targets_result = self._path_finder.find_common_reachable_targets(
    #     #     graph=current_graph,
    #     #     mutable_path=self._mutable_path,
    #     #     source_nodes=source_nodes,
    #     #     candidate_target_nodes=candidate_target_nodes,
    #     #     weight_function=DunderNameWeightFunction(),
    #     #     max_path_weight=1,
    #     # )
    #
    #     # for stop_event in self._path_finder.traverse_dfs(
    #     #     graph=current_graph,
    #     #     mutable_path=self._mutable_path,
    #     #     source_node=source_nodes,
    #     #     target_nodes=target_nodes,
    #     #     weight_function=DunderNameWeightFunction(),
    #     #     max_path_weight=DunderNameWeightFunction.MAX_ENTITY_LINKS,
    #     #     allow_node_revisits=True,
    #     # ):
    #     #     path = stop_event.current_path
    #     #     attribute_descriptor = mutable_path.attribute_computation.attribute_descriptor
    #
    #     # if self._verbose_debug_logs:
    #     #     logger.debug(
    #     #         LazyFormat(
    #     #             "Completed search for reachable targets",
    #     #             reachable_target_count=len(common_reachable_targets_result.reachable_targets),
    #     #             result=common_reachable_targets_result,
    #     #         )
    #     #     )
    #     # attribute_computation_update = AttributeComputationUpdate(
    #     #     # dundered_name_element_addition=metric_name,
    #     #     source_model_id_additions=tuple(semantic_model_ids),
    #     #     # linkable_element_property_additions=(LinkableElementProperty.METRIC,),
    #     #     # element_type_addition=LinkableElementType.METRIC,
    #     # )
    #     # metric_attribute_node = MetricAttributeNode.get_instance(metric_name)
    #     # for reachable_dsi_entity_node in common_reachable_targets_result.reachable_targets:
    #     #     subgraph.add_edge(
    #     #         EntityAttributeEdge.get_instance(
    #     #             tail_node=reachable_dsi_entity_node,
    #     #             head_node=metric_attribute_node,
    #     #             attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
    #     #             attribute_computation_update=attribute_computation_update,
    #     #         )
    #     #     )
    #     #
    #     # self._metric_name_to_connected_dsi_entity_nodes[
    #     #     metric_name
    #     # ] = common_reachable_targets_result.reachable_targets.as_frozen()
    #     # self._metric_name_to_derivative_semantic_model_ids[metric_name] = semantic_model_ids.as_frozen()

    @override
    def generate_subgraph(self, current_graph: SemanticGraph) -> MutableSemanticGraph:
        self._current_graph = current_graph
        self._traversable_nodes_for_dsi_entity_node_search = current_graph.nodes_with_label(
            LocalModelLabel.get_instance()
        ).union(current_graph.nodes_with_label(DsiEntityLabel.get_instance()))
        self._measure_name_to_edge_argument.clear()

        # current_subgraph = MutableSemanticGraph.create()

        generate_subgraph_context = _GenerateGroupByMetricSubgraphContext(
            current_graph=current_graph,
            path_finder=self._path_finder,
            attribute_resolver=AttributeResolver(
                manifest_object_lookup=self._manifest_object_lookup,
                semantic_graph=current_graph,
                path_finder=self._path_finder,
            ),
        )

        if self._verbose_debug_logs:
            logger.debug(LazyFormat("Starting with graph", current_graph=current_graph))

        # for metric in self._manifest_object_lookup.get_metrics():
        #     if metric.name not in self._metric_name_to_edge_argument:
        #         self._generate_subgraph_for_any_metric(current_subgraph, metric)

        return generate_subgraph_context.generate()


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
