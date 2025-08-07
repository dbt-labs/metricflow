from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Set
from functools import cached_property
from typing import Mapping, Optional

from typing_extensions import override

from metricflow_semantics.experimental.metricflow_exception import MetricflowInternalError
from metricflow_semantics.experimental.mf_graph.path_finding.pathfinder import MetricflowPathfinder
from metricflow_semantics.experimental.mf_graph.path_finding.traversal_profile_differ import TraversalProfileDiffer
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_recipe import IndexedDunderName
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.recipe_writer_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.experimental.semantic_graph.edges.edge_labels import (
    DenyEntityKeyQueryResolutionLabel,
    DenyVisibleAttributesLabel,
)
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.entity_nodes import MetricTimeNode
from metricflow_semantics.experimental.semantic_graph.nodes.node_labels import (
    BaseMetricLabel,
    ConfiguredEntityLabel,
    JoinedModelLabel,
    LocalModelLabel,
    MeasureLabel,
    MetricLabel,
)
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import (
    SemanticGraph,
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.trie_resolver.dunder_name_descriptor import (
    DunderNameDescriptor,
    EntityKeyQueryForGroupByMetric,
)
from metricflow_semantics.experimental.semantic_graph.trie_resolver.dunder_name_trie import (
    DunderNameTrie,
    MutableDunderNameTrie,
)
from metricflow_semantics.experimental.semantic_graph.trie_resolver.dunder_name_trie_resolver import (
    DunderNameTrieResolver,
    TrieResolutionResult,
)
from metricflow_semantics.experimental.semantic_graph.trie_resolver.entity_key_resolver import (
    EntityKeyTrieResolver,
)
from metricflow_semantics.helpers.performance_helpers import ExecutionTimer
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType

logger = logging.getLogger(__name__)


class GroupByMetricTrieResolver(DunderNameTrieResolver):
    """Resolves the group-by metrics that are available for the given measures / metrics.

    Group-by metrics are handled separately to break out the resolution of all available group-by items into smaller
    pieces.
    """

    def __init__(  # noqa: D107
        self,
        semantic_graph: SemanticGraph,
        path_finder: MetricflowPathfinder[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath],
    ) -> None:
        super().__init__(semantic_graph=semantic_graph, path_finder=path_finder)
        self._local_model_nodes = semantic_graph.nodes_with_labels(self._local_model_label)
        self._key_attribute_nodes = semantic_graph.nodes_with_labels(self._entity_key_attribute_label)
        self._node_allow_set = self._local_model_nodes.union(
            self._key_attribute_nodes,
            self._semantic_graph.nodes_with_labels(
                JoinedModelLabel.get_instance(),
                ConfiguredEntityLabel.get_instance(),
            ),
        )
        self._metric_nodes = semantic_graph.nodes_with_labels(MetricLabel.get_instance())
        self._measure_nodes = semantic_graph.nodes_with_labels(MeasureLabel.get_instance())
        self._base_metric_nodes = semantic_graph.nodes_with_labels(BaseMetricLabel.get_instance())
        self._nodes_in_path_from_metric_nodes_to_local_model_nodes = semantic_graph.nodes_with_labels(
            MetricLabel.get_instance(),
            MeasureLabel.get_instance(),
            LocalModelLabel.get_instance(),
        )
        self._metric_time_node = MetricTimeNode.get_instance()

        self._group_by_metric_element_properties = (
            LinkableElementProperty.JOINED,
            LinkableElementProperty.METRIC,
        )

        self._verbose_debug_logs = False

    @override
    def resolve_trie(
        self, source_nodes: OrderedSet[SemanticGraphNode], element_filter: Optional[LinkableElementFilter]
    ) -> TrieResolutionResult:
        execution_timer = ExecutionTimer()
        pathfinder_profile_differ = TraversalProfileDiffer(self._path_finder)

        with execution_timer, pathfinder_profile_differ:
            trie_result = self._resolve_trie(source_nodes=source_nodes, element_filter=element_filter)

        return TrieResolutionResult(
            duration=execution_timer.total_duration,
            traversal_profile=pathfinder_profile_differ.profile_delta,
            dunder_name_trie=trie_result,
        )

    @cached_property
    def _metric_name_to_entity_key_trie(self) -> Mapping[str, DunderNameTrie]:
        return _MetricNameToEntityKeyTrieGenerator(
            semantic_graph=self._semantic_graph,
            path_finder=self._path_finder,
            model_node_to_entity_key_trie=self._model_node_to_entity_key_trie,
        ).generate()

    @cached_property
    def _entity_key_name_to_metric_names(self) -> Mapping[str, OrderedSet[str]]:
        entity_key_name_to_metric_names: dict[str, MutableOrderedSet[str]] = defaultdict(MutableOrderedSet)
        for metric_name, entity_key_trie in self._metric_name_to_entity_key_trie.items():
            for indexed_name, _ in entity_key_trie.name_items():
                entity_key_name_to_metric_names[indexed_name[-1]].add(metric_name)

        return entity_key_name_to_metric_names

    def _resolve_trie(
        self,
        source_nodes: OrderedSet[SemanticGraphNode],
        element_filter: Optional[LinkableElementFilter],
    ) -> DunderNameTrie:
        trie_to_update = MutableDunderNameTrie()

        # Find all required local-model nodes.
        metric_names_allow_set: Optional[Set[str]] = None

        if element_filter is not None:
            metric_names_allow_set = element_filter.element_names
            # Group-by metrics always have these properties, so return an empty set if the filter doesn't allow them.
            if not element_filter.allow(
                element_name=None, element_properties=(LinkableElementProperty.METRIC, LinkableElementProperty.JOINED)
            ):
                logger.debug(
                    LazyFormat(
                        "Skipping resolution due to the element filter",
                        class_name=self.__class__.__name__,
                        element_filter=element_filter,
                    )
                )
                return trie_to_update

        find_descendants_result = self._path_finder.find_descendants(
            graph=self._semantic_graph,
            source_nodes=source_nodes,
            target_nodes=self._local_model_nodes,
            node_allow_set=self._node_allow_set,
            deny_labels={self._deny_visible_attributes_label},
        )
        reachable_local_model_nodes = tuple(find_descendants_result.reachable_target_nodes)
        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Found local-model nodes for source nodes.",
                    source_nodes=source_nodes,
                    reachable_local_model_nodes=reachable_local_model_nodes,
                )
            )

        if len(reachable_local_model_nodes) == 0:
            raise MetricflowInternalError(
                LazyFormat(
                    "No local-model nodes were found to be reachable from the given source nodes.",
                    source_nodes=source_nodes,
                    local_model_nodes=self._local_model_nodes,
                )
            )

        # Figure out which entity-key queries are possible given the local-model nodes.
        entity_key_trie: DunderNameTrie = MutableDunderNameTrie.intersection_merge_common(
            tuple(
                self._model_node_to_entity_key_trie[reachable_local_model_node]
                for reachable_local_model_node in reachable_local_model_nodes
            )
        )
        # `entity_key_trie` contains all entity-keys like `booking__listing__user`. But we only want `booking`,
        # so use 1 as the max length.
        collected_entity_key_names: list[str] = []
        collected_derived_from_model_ids: list[SemanticModelId] = []

        for indexed_dunder_name, descriptor in entity_key_trie.name_items(max_length=1):
            if len(indexed_dunder_name) != 1:
                raise MetricflowInternalError(
                    LazyFormat(
                        "The indexed dunder name should have only contained 1 element.",
                        indexed_dunder_name=indexed_dunder_name,
                    )
                )
            collected_entity_key_names.append(indexed_dunder_name[0])
            collected_derived_from_model_ids.extend(descriptor.derived_from_model_ids)

        entity_key_names = FrozenOrderedSet(collected_entity_key_names)
        derived_from_model_ids = FrozenOrderedSet(collected_derived_from_model_ids)

        for entity_key_name in entity_key_names:
            metric_names = self._entity_key_name_to_metric_names[entity_key_name]
            if metric_names is None:
                logger.warning(
                    LazyFormat(
                        "Valid group-by metrics for the given entity-key name not found. this may occur"
                        " when a given entity is not accessible from any metric (known case is related to conversion"
                        " measures)",
                        entity_key_name=entity_key_name,
                        known_key_names=list(self._entity_key_name_to_metric_names.keys()),
                    )
                )
            trie_for_entity = trie_to_update.next_name_element_to_trie[entity_key_name]

            items_to_add_to_trie: list[tuple[IndexedDunderName, DunderNameDescriptor]] = []

            for metric_name in metric_names:
                if metric_names_allow_set is not None and metric_name not in metric_names_allow_set:
                    continue
                entity_key_queries: list[EntityKeyQueryForGroupByMetric] = []
                for indexed_dunder_name, descriptor in self._metric_name_to_entity_key_trie[metric_name].name_items():
                    if indexed_dunder_name[-1] != entity_key_name:
                        continue
                    entity_key_queries.append(
                        EntityKeyQueryForGroupByMetric(
                            entity_key_query=indexed_dunder_name,
                            derived_from_model_ids=descriptor.derived_from_model_ids,
                        )
                    )
                descriptor = DunderNameDescriptor(
                    element_type=LinkableElementType.METRIC,
                    time_grain=None,
                    date_part=None,
                    element_properties=self._group_by_metric_element_properties,
                    origin_model_ids=self._virtual_semantic_model_ids,
                    derived_from_model_ids=tuple(derived_from_model_ids),
                    entity_key_queries_for_group_by_metric=tuple(entity_key_queries),
                )
                items_to_add_to_trie.append(((metric_name,), descriptor))
            trie_for_entity.add_name_items(items_to_add_to_trie)

        return trie_to_update

    @cached_property
    def _model_node_to_entity_key_trie(self) -> Mapping[SemanticGraphNode, DunderNameTrie]:
        resolver = EntityKeyTrieResolver(self._semantic_graph)
        resolver_result = resolver.resolve_entity_key_trie_mapping()
        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Resolved entity-key trie",
                    resolver_result=resolver_result,
                )
            )
        return resolver_result


class _MetricNameToEntityKeyTrieGenerator:
    def __init__(
        self,
        semantic_graph: SemanticGraph,
        path_finder: MetricflowPathfinder[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath],
        model_node_to_entity_key_trie: Mapping[SemanticGraphNode, DunderNameTrie],
    ) -> None:
        self._current_graph = semantic_graph
        self._path_finder = path_finder
        self._metric_node_to_entity_key_trie: dict[SemanticGraphNode, MutableDunderNameTrie] = {}
        metric_nodes = semantic_graph.nodes_with_labels(MetricLabel.get_instance())
        self._metric_nodes_in_current_graph = metric_nodes
        self._verbose_debug_logs = False
        self._model_node_to_entity_key_trie = model_node_to_entity_key_trie

    def _get_entity_key_trie_for_metric_node(
        self,
        metric_node: SemanticGraphNode,
        _metric_nodes_in_definition_path: Set[SemanticGraphNode] = frozenset(),
    ) -> MutableDunderNameTrie:
        entity_key_trie = self._metric_node_to_entity_key_trie.get(metric_node)
        if entity_key_trie is not None:
            return entity_key_trie

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
                parent_metric_nodes = MutableOrderedSet()
                break
            else:
                parent_metric_nodes.add(parent_metric_node)

        if len(parent_metric_nodes) == 0:
            result = MutableDunderNameTrie()
            self._metric_node_to_entity_key_trie[metric_node] = result
            return result

        key_query_sets_to_intersect: list[MutableDunderNameTrie] = []
        for parent_metric_node in parent_metric_nodes:
            if parent_metric_node not in self._metric_node_to_entity_key_trie:
                self._get_entity_key_trie_for_metric_node(
                    metric_node=parent_metric_node,
                    _metric_nodes_in_definition_path=_metric_nodes_in_definition_path | {metric_node},
                )
            key_query_sets_to_intersect.append(self._metric_node_to_entity_key_trie[parent_metric_node])

        dunder_name_trie = MutableDunderNameTrie.intersection_merge_common(key_query_sets_to_intersect)
        self._metric_node_to_entity_key_trie[metric_node] = dunder_name_trie

        return dunder_name_trie

    def generate(self) -> Mapping[str, MutableDunderNameTrie]:
        """This return a mapping from the metric name, to a trie representing the valid entity-key queries for that metric.

        The descriptors for the trie include context on the semantic models required to query the entity key for the
        given metric.
        """
        current_graph = self._current_graph
        path_finder = self._path_finder

        base_metric_label = BaseMetricLabel.get_instance()
        base_metric_nodes = current_graph.nodes_with_labels(base_metric_label)

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

        self._metric_node_to_entity_key_trie = {}

        local_model_nodes = current_graph.nodes_with_labels(LocalModelLabel.get_instance())
        measure_nodes = current_graph.nodes_with_labels(MeasureLabel.get_instance())

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
            # For metrics that require metric time to be in a query, it's not possible to query just the entity keys.
            DenyEntityKeyQueryResolutionLabel.get_instance(),
            # For conversion metrics, the conversion measure is effectively hidden and is not used to generate the
            # available group-by items for a metric.
            DenyVisibleAttributesLabel.get_instance(),
        }

        # For each base metric, generate the set of possible entity-key queries. These will be used to generate the
        # set of possible entity-key queries for derived metrics.
        for base_metric_node in base_metric_nodes:
            base_metric_node_set = FrozenOrderedSet((base_metric_node,))

            result = path_finder.find_descendants(
                graph=current_graph,
                source_nodes=base_metric_node_set,
                target_nodes=local_model_nodes,
                node_allow_set=allowed_nodes_for_walking_from_metrics_to_models,
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
                self._metric_node_to_entity_key_trie[base_metric_node] = MutableDunderNameTrie()
                continue

            entity_key_trie_intersection_list: list[DunderNameTrie] = []
            for source_model_node in visible_source_model_nodes:
                entity_key_trie_intersection_list.append(self._model_node_to_entity_key_trie[source_model_node])

            # Figure out the models that a metric depends on.
            result = path_finder.find_descendants(
                graph=current_graph,
                source_nodes=base_metric_node_set,
                target_nodes=local_model_nodes,
                node_allow_set=allowed_nodes_for_walking_from_metrics_to_models,
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
                model_id = model_node.recipe_step_to_append.add_model_join
                assert model_id is not None, LazyFormat(
                    "A found node is expected to be a model node, but the recipe step does not include the"
                    " associated model ID. The search may be incorrect and found the wrong node.",
                    model_id=model_id,
                    model_node=model_node,
                )
                common_source_model_ids.add(model_id)

            dunder_name_trie = MutableDunderNameTrie.intersection_merge_common(entity_key_trie_intersection_list)

            dunder_name_trie.update_derived_from_model_ids(
                tuple(common_source_model_ids),
            )
            self._metric_node_to_entity_key_trie[base_metric_node] = dunder_name_trie

        metric_name_to_entity_key_trie: dict[str, MutableDunderNameTrie] = {}

        for metric_node in self._metric_nodes_in_current_graph:
            entity_key_trie = self._get_entity_key_trie_for_metric_node(metric_node)
            metric_name = metric_node.dunder_name_element
            if metric_name is None:
                raise RuntimeError(
                    LazyFormat(
                        "Expected a metric node to have a name",
                        metric_node=metric_node,
                    )
                )

            metric_name_to_entity_key_trie[metric_name] = entity_key_trie

        return metric_name_to_entity_key_trie
