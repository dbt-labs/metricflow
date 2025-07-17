from __future__ import annotations

import logging
from collections.abc import Set
from dataclasses import dataclass
from functools import cached_property

from typing_extensions import override

from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import AttributeRecipeUpdate
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_resolver import (
    AttributeResolver,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.key_query_resolver import (
    DsiEntityKeyQueryResolver,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.key_query_set import (
    DsiEntityKeyQueryGroup,
)
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
    BaseMetricLabel,
    DenyEntityKeyQueryResolutionLabel,
    DenyVisibleAttributesLabel,
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
from metricflow_semantics.mf_logging.runtime import log_block_runtime

logger = logging.getLogger(__name__)


class GroupByMetricSubgraphGenerator(SemanticSubgraphGenerator):
    def __init__(self, argument_set: SubgraphGeneratorArgumentSet) -> None:
        super().__init__(argument_set)
        self._mutable_path = AttributeRecipeWriterPath.create()
        self._verbose_debug_logs = False
        self._metric_name_to_connected_dsi_entity_nodes: dict[str, FrozenOrderedSet[SemanticGraphNode]] = {}
        self._metric_name_to_derivative_semantic_model_ids: dict[str, FrozenOrderedSet[SemanticModelId]] = {}

        self._current_graph: SemanticGraph = MutableSemanticGraph.create()
        self._traversable_nodes_for_dsi_entity_node_search: OrderedSet[SemanticGraphNode] = FrozenOrderedSet()
        # self._measure_name_to_edge_argument: dict[str, MetricAttributeEdgeArgument] = {}
        # self._metric_name_to_edge_argument: dict[str, MetricAttributeEdgeArgument] = {}

        current_graph = MutableSemanticGraph.create()
        self._generate_call_context = _GenerateGroupByMetricSubgraphContext(
            current_graph=current_graph,
            path_finder=self._path_finder,
            attribute_resolver=AttributeResolver(self._manifest_object_lookup, current_graph, self._path_finder),
        )

    @override
    def generate_subgraph(self, current_graph: SemanticGraph) -> MutableSemanticGraph:
        self._current_graph = current_graph
        self._traversable_nodes_for_dsi_entity_node_search = current_graph.nodes_with_label(
            LocalModelLabel.get_instance()
        ).union(current_graph.nodes_with_label(DsiEntityLabel.get_instance()))
        # self._measure_name_to_edge_argument.clear()

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
        self._metric_node_to_key_query_group: dict[SemanticGraphNode, DsiEntityKeyQueryGroup] = {}
        metric_nodes = current_graph.nodes_with_label(MetricLabel.get_instance())
        self._metric_nodes_in_current_graph = metric_nodes
        self._verbose_debug_logs = False

    @cached_property
    def _model_node_to_key_query_group(self) -> dict[SemanticGraphNode, DsiEntityKeyQueryGroup]:
        with log_block_runtime("Resolve key queries"):
            resolver = DsiEntityKeyQueryResolver()
            resolver_result = resolver.find_key_query_groups(self._current_graph)
            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Resolved key query groups",
                        resolver_result=resolver_result,
                    )
                )
            return resolver_result

    def _get_key_query_group_for_metric_node(
        self,
        metric_node: SemanticGraphNode,
        _metric_nodes_in_definition_path: Set[SemanticGraphNode] = frozenset(),
    ) -> DsiEntityKeyQueryGroup:
        entity_key_query_set = self._metric_node_to_key_query_group.get(metric_node)
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

        if metric_node not in self._metric_nodes_in_current_graph:
            raise RuntimeError(
                LazyFormat(
                    "Traversal reached a non-metric node",
                    current_node=metric_node,
                    metric_nodes=self._metric_nodes_in_current_graph,
                    metric_nodes_in_definition_path=_metric_nodes_in_definition_path,
                )
            )

        current_graph = self._current_graph

        deny_label = DenyEntityKeyQueryResolutionLabel.get_instance()
        parent_metric_nodes: MutableOrderedSet[SemanticGraphNode] = MutableOrderedSet()
        for edge_to_successor in current_graph.edges_with_tail_node(metric_node):
            parent_metric_node = edge_to_successor.head_node
            if deny_label in edge_to_successor.labels or deny_label in edge_to_successor.head_node.labels:
                parent_metric_nodes.clear()
                break
            else:
                parent_metric_nodes.add(parent_metric_node)

        if len(parent_metric_nodes) == 0:
            result = DsiEntityKeyQueryGroup()
            self._metric_node_to_key_query_group[metric_node] = result
            return result

        key_query_sets_to_intersect = []
        for parent_metric_node in parent_metric_nodes:
            if parent_metric_node not in self._metric_node_to_key_query_group:
                self._get_key_query_group_for_metric_node(
                    metric_node=parent_metric_node,
                    _metric_nodes_in_definition_path=_metric_nodes_in_definition_path | {metric_node},
                )
            key_query_sets_to_intersect.append(self._metric_node_to_key_query_group[parent_metric_node])
        intersected_key_query_set = DsiEntityKeyQueryGroup.intersection(key_query_sets_to_intersect)
        self._metric_node_to_key_query_group[metric_node] = intersected_key_query_set

        return intersected_key_query_set

    def generate(self) -> MutableSemanticGraph:
        current_graph = self._current_graph
        path_finder = self._path_finder

        base_metric_label = BaseMetricLabel.get_instance()
        base_metric_nodes = current_graph.nodes_with_label(base_metric_label)

        if len(base_metric_nodes) == 0:
            raise RuntimeError(
                LazyFormat(
                    "Did not find any base metric nodes. This indicates an error in graph construction.",
                    base_metric_nodes=base_metric_nodes,
                    label=base_metric_label,
                )
            )

        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Found base-metric nodes",
                    base_metric_nodes=base_metric_nodes,
                )
            )

        self._metric_node_to_key_query_group.clear()

        # allowed_nodes_for_walking_from_metrics_to_models = allowed_nodes_for_walking_between_measures_and_metrics.union(
        #     local_model_nodes
        # )
        local_model_nodes = current_graph.nodes_with_label(LocalModelLabel.get_instance())
        measure_nodes = current_graph.nodes_with_label(MeasureLabel.get_instance())

        allowed_nodes_for_walking_from_metrics_to_models: MutableOrderedSet[SemanticGraphNode] = MutableOrderedSet(
            local_model_nodes
        )
        allowed_nodes_for_walking_from_metrics_to_models.update(self._metric_nodes_in_current_graph)
        allowed_nodes_for_walking_from_metrics_to_models.update(measure_nodes)

        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Finding entity key queries for base metric nodes",
                    base_metric_nodes=base_metric_nodes,
                    local_model_nodes=local_model_nodes,
                    allowed_nodes_for_walking_from_metrics_to_models=allowed_nodes_for_walking_from_metrics_to_models,
                )
            )

        deny_labels = {
            # For conversion metrics, the conversion measure is effectively hidden and is not used to generate the
            # available group-by items for a metric.
            DenyEntityKeyQueryResolutionLabel.get_instance(),
            # For metrics that require metric time to be in a query, it's not possible to query just the entity keys.
            DenyVisibleAttributesLabel.get_instance(),
        }

        # For each base metric, generate the set of possible entity-key queries. These will be used to generate the
        # set of possible entity-key queries for derived metrics.
        for base_metric_node in base_metric_nodes:
            base_metric_node_set = FrozenOrderedSet((base_metric_node,))

            result = path_finder.find_reachable_targets(
                graph=current_graph,
                source_nodes=base_metric_node_set,
                candidate_target_nodes=local_model_nodes,
                allowed_successor_nodes=allowed_nodes_for_walking_from_metrics_to_models,
                deny_labels=deny_labels,
            )
            visible_source_model_nodes = result.reachable_target_nodes

            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Found model nodes containing the measures for the given metric. Those model nodes"
                        " will be used for generating the available group-by items.",
                        base_metric_node=base_metric_node,
                        visible_source_model_nodes=visible_source_model_nodes,
                    )
                )

            if len(visible_source_model_nodes) == 0:
                # Entity-key attributes can't be queried for cumulative metrics (as described by the deny labels),
                # so set an empty result.
                self._metric_node_to_key_query_group[base_metric_node] = DsiEntityKeyQueryGroup()
                continue

            key_query_groups_to_intersect: list[DsiEntityKeyQueryGroup] = []
            for source_model_node in visible_source_model_nodes:
                key_query_groups_to_intersect.append(self._model_node_to_key_query_group[source_model_node])

            # Figure out the models that a metric depends on.
            result = path_finder.find_reachable_targets(
                graph=current_graph,
                source_nodes=base_metric_node_set,
                candidate_target_nodes=local_model_nodes,
                allowed_successor_nodes=allowed_nodes_for_walking_from_metrics_to_models,
            )
            source_model_nodes = result.reachable_target_nodes
            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Found model nodes containing the measures for the given metric. Those model nodes"
                        " will be used for generating the available group-by items.",
                        base_metric_node=base_metric_node,
                        visible_source_model_nodes=visible_source_model_nodes,
                    )
                )

            common_source_model_ids: MutableOrderedSet[SemanticModelId] = MutableOrderedSet()
            for model_node in source_model_nodes - visible_source_model_nodes:
                model_id = model_node.recipe_update.join_model
                assert model_id is not None, LazyFormat(
                    "Model node is missing the model ID in the recipe update",
                    model_id=model_id,
                    model_node=model_node,
                )
                common_source_model_ids.add(model_id)

            intersected_key_query_group = DsiEntityKeyQueryGroup.intersection(key_query_groups_to_intersect)
            self._metric_node_to_key_query_group[
                base_metric_node
            ] = intersected_key_query_group.with_common_source_models(
                tuple(common_source_model_ids),
            )

        metric_name_to_key_query_group: dict[str, DsiEntityKeyQueryGroup] = {}
        subgraph = MutableSemanticGraph.create()

        for metric_node in self._metric_nodes_in_current_graph:
            key_query_group = self._get_key_query_group_for_metric_node(metric_node)
            metric_name = metric_node.dunder_name_element
            if metric_name is None:
                raise RuntimeError(
                    LazyFormat(
                        "Expected a metric node to have a name",
                        metric_node=metric_node,
                    )
                )

            metric_name_to_key_query_group[metric_name] = key_query_group
            group_by_metric_node = GroupByMetricNode.get_instance(metric_name)

            for key_name in key_query_group.key_names:
                key_entity_node = KeyEntityNode.get_instance(key_name)
                subgraph.add_edge(
                    EntityAttributeEdge.get_instance(
                        tail_node=key_entity_node,
                        attribute_edge_type=AttributeEdgeType.ENTITY_TO_ATTRIBUTE,
                        head_node=group_by_metric_node,
                        attribute_recipe_update=AttributeRecipeUpdate(
                            add_dunder_name_element=key_name,
                            add_entity_link=key_name,
                            provide_key_query_group=key_query_group.filter_by_key_name(key_name),
                        ),
                    )
                )

        return subgraph
