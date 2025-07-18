from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import cached_property
from typing import Optional, Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.collection_helpers.syntactic_sugar import (
    mf_first_item,
    mf_first_non_none_or_raise,
    mf_flatten,
)
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.dsi.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.metricflow_exception import MetricflowInternalError
from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.mf_graph.path_finding.path_finder import (
    MetricflowGraphPathFinder,
)
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.annotated_spec_linkable_element_set import (
    AnnotatedSpecLinkableElementSet,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_recipe import AttributeQueryRecipe
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.key_query_set import (
    KeyQueryGroup,
)
from metricflow_semantics.experimental.semantic_graph.edges.edge_labels import (
    CumulativeMeasureLabel,
    DenyDatePartLabel,
    DenyVisibleAttributesLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.node_labels import (
    ConfiguredEntityLabel,
    GroupByAttributeLabel,
    GroupByMetricLabel,
    JoinedModelLabel,
    KeyEntityClusterLabel,
    LocalModelLabel,
    MeasureLabel,
    MetricLabel,
    MetricTimeLabel,
    TimeClusterLabel,
)
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import (
    SemanticGraph,
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.weight.dunder_name_weight import DunderNameWeightFunction
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.model.semantics.linkable_element_set_base import AnnotatedSpec
from metricflow_semantics.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS

logger = logging.getLogger(__name__)


class AttributeResolver:
    def __init__(
        self,
        manifest_object_lookup: ManifestObjectLookup,
        semantic_graph: SemanticGraph,
        path_finder: MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath],
    ) -> None:
        self._manifest_object_lookup = manifest_object_lookup
        self._semantic_graph = semantic_graph
        self._path_finder = path_finder
        self._mutable_path = AttributeRecipeWriterPath.create()
        self._verbose_debug_logs = False
        self._cumulative_measure_label = CumulativeMeasureLabel.get_instance()
        self._deny_date_part_label = DenyDatePartLabel.get_instance()
        # Cache for results.
        self._source_nodes_to_element_set: dict[ElementSetCacheKey, AnnotatedSpecLinkableElementSet] = {}

    def _generate_non_metric_time_recipes(
        self, model_node: SemanticGraphNode, element_filter: Optional[LinkableElementFilter]
    ) -> Sequence[AttributeQueryRecipe]:
        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Generating non-metric-time recipes",
                    model_node=model_node,
                )
            )

        node_deny_set: Optional[OrderedSet[SemanticGraphNode]] = None
        if element_filter is not None and LinkableElementProperty.METRIC in element_filter.without_any_of:
            node_deny_set = self._group_by_metric_nodes
        return self._remove_ambiguous_recipes(
            self._generate_recipes(
                model_node,
                node_allow_set=None,
                element_filter=element_filter,
                node_deny_set=node_deny_set,
            )
        )

    def _generate_metric_time_recipes(
        self, measure_node: SemanticGraphNode, element_filter: Optional[LinkableElementFilter]
    ) -> Sequence[AttributeQueryRecipe]:
        allowed_nodes = self._traversable_nodes_for_metric_time_recipes
        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Generating metric-time recipes",
                    measure_node=measure_node,
                    allowed_nodes=allowed_nodes,
                )
            )
        return self._remove_ambiguous_recipes(
            self._generate_recipes(
                source_node=measure_node,
                node_allow_set=allowed_nodes,
                element_filter=element_filter,
                node_deny_set=self._group_by_metric_nodes,
            )
        )

    def resolve_metric_time_specs(
        self, element_filter: Optional[LinkableElementFilter]
    ) -> AnnotatedSpecLinkableElementSet:
        matching_metric_time_nodes = self._semantic_graph.nodes_with_label(MetricTimeLabel.get_instance())
        if len(matching_metric_time_nodes) != 1:
            raise MetricflowInternalError(
                LazyFormat(
                    "Did not find exactly 1 metric-time node in the semantic graph",
                    matching_metric_time_nodes=matching_metric_time_nodes,
                )
            )
        metric_time_node = mf_first_item(matching_metric_time_nodes)

        return AnnotatedSpecLinkableElementSet.create_from_annotated_specs(
            self._generate_specs_from_recipes(
                self._generate_recipes(
                    source_node=metric_time_node,
                    element_filter=element_filter,
                    node_allow_set=self._semantic_graph.nodes_with_label(TimeClusterLabel.get_instance()),
                    node_deny_set=self._group_by_metric_nodes,
                ),
                element_filter=element_filter,
            )
        )

    def _should_filter_date_part(
        self, source_nodes: OrderedSet[SemanticGraphNode], collected_labels: OrderedSet[MetricflowGraphLabel]
    ) -> bool:
        if self._deny_date_part_label not in collected_labels:
            return False

        for source_node in source_nodes:
            for successor_edge in self._semantic_graph.edges_with_tail_node(source_node):
                if self._cumulative_measure_label in successor_edge.labels:
                    return True
        return False

    def resolve_annotated_specs(
        self, source_nodes: OrderedSet[SemanticGraphNode], element_filter: Optional[LinkableElementFilter]
    ) -> AnnotatedSpecLinkableElementSet:
        cache_key = ElementSetCacheKey(nodes=source_nodes.as_frozen(), element_filter=element_filter)
        result = self._source_nodes_to_element_set.get(cache_key)
        if result is not None:
            return result

        search_result = self._find_nearest_measure_nodes(source_nodes)
        measure_nodes = search_result.measure_nodes
        collected_labels = search_result.collected_labels

        if len(measure_nodes) == 0:
            raise RuntimeError(
                LazyFormat(
                    "No reachable measure nodes were found from the source nodes. This indicates an"
                    " error in the semantic graph or traversal.",
                    measure_nodes=measure_nodes,
                    source_nodes=source_nodes,
                )
            )

        model_nodes = self._find_nearest_local_semantic_model_nodes(measure_nodes)

        if len(model_nodes) == 0:
            raise RuntimeError(
                LazyFormat(
                    "No reachable model nodes were found for the measure nodes. This indicates an"
                    " error in the semantic graph or traversal.",
                    measure_nodes=measure_nodes,
                    model_nodes=model_nodes,
                )
            )
        # TODO: Think about metric-time-grain mismatch for conversion measures.

        if self._should_filter_date_part(source_nodes=source_nodes, collected_labels=collected_labels):
            if element_filter is None:
                default_filter = LinkableElementFilter()
                element_filter = default_filter.copy(
                    without_any_of=default_filter.without_any_of.union((LinkableElementProperty.DATE_PART,))
                )
            else:
                element_filter = element_filter.copy(
                    without_any_of=element_filter.without_any_of.union((LinkableElementProperty.DATE_PART,))
                )

        model_element_sets_to_intersect: list[AnnotatedSpecLinkableElementSet] = []
        for model_node in model_nodes:
            cache_key = ElementSetCacheKey(nodes=FrozenOrderedSet((model_node,)), element_filter=element_filter)

            element_set = self._source_nodes_to_element_set.get(cache_key)
            if element_set is not None:
                model_element_sets_to_intersect.append(element_set)
                continue

            non_metric_time_recipes = self._remove_ambiguous_recipes(
                self._generate_non_metric_time_recipes(model_node, element_filter=element_filter)
            )
            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Generated non-metric-time recipes",
                        model_node=model_node,
                        non_metric_time_recipes=non_metric_time_recipes,
                    )
                )
            element_set = AnnotatedSpecLinkableElementSet.create_from_annotated_specs(
                self._generate_specs_from_recipes(non_metric_time_recipes, element_filter=element_filter)
            )
            model_element_sets_to_intersect.append(element_set)

            self._source_nodes_to_element_set[cache_key] = element_set

        metric_time_elements_sets_to_intersect: list[AnnotatedSpecLinkableElementSet] = []
        for measure_node in measure_nodes:
            cache_key = ElementSetCacheKey(nodes=FrozenOrderedSet((measure_node,)), element_filter=element_filter)
            element_set = self._source_nodes_to_element_set.get(cache_key)
            if element_set is not None:
                metric_time_elements_sets_to_intersect.append(element_set)
                continue

            metric_time_recipes = self._generate_metric_time_recipes(
                measure_node=measure_node,
                element_filter=element_filter,
            )
            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat(
                        "Generated metric-time recipes",
                        model_node=measure_node,
                        metric_time_recipes=metric_time_recipes,
                    )
                )
            element_set = AnnotatedSpecLinkableElementSet.create_from_annotated_specs(
                self._generate_specs_from_recipes(metric_time_recipes, element_filter=element_filter)
            )
            self._source_nodes_to_element_set[cache_key] = element_set
            metric_time_elements_sets_to_intersect.append(element_set)

        if len(model_element_sets_to_intersect) == 0:
            model_node_element_set = AnnotatedSpecLinkableElementSet()
        else:
            model_node_element_set = model_element_sets_to_intersect[0].intersection(
                *model_element_sets_to_intersect[1:]
            )

        if len(metric_time_elements_sets_to_intersect) == 0:
            metric_time_element_set = AnnotatedSpecLinkableElementSet()
        else:
            metric_time_element_set = metric_time_elements_sets_to_intersect[0].intersection(
                *metric_time_elements_sets_to_intersect[1:]
            )

        result = model_node_element_set.union(metric_time_element_set)
        cache_key = ElementSetCacheKey(nodes=source_nodes.as_frozen(), element_filter=element_filter)
        self._source_nodes_to_element_set[cache_key] = result
        return result

    @cached_property
    def _group_by_metric_nodes(self) -> OrderedSet[SemanticGraphNode]:
        return self._semantic_graph.nodes_with_label(GroupByMetricLabel.get_instance())

    @cached_property
    def _traversable_nodes_for_finding_measure_nodes(self) -> OrderedSet[SemanticGraphNode]:
        nodes = MutableOrderedSet[SemanticGraphNode]()

        for label in (
            MetricLabel.get_instance(),
            MeasureLabel.get_instance(),
        ):
            nodes.update(self._semantic_graph.nodes_with_label(label))
        return nodes

    @cached_property
    def _traversable_nodes_for_finding_local_semantic_model_nodes(self) -> OrderedSet[SemanticGraphNode]:
        nodes = MutableOrderedSet[SemanticGraphNode]()

        for label in (
            GroupByMetricLabel.get_instance(),
            MeasureLabel.get_instance(),
            LocalModelLabel.get_instance(),
        ):
            nodes.update(self._semantic_graph.nodes_with_label(label))
        return nodes

    @cached_property
    def _traversable_nodes_for_metric_time_recipes(self) -> OrderedSet[SemanticGraphNode]:
        nodes = MutableOrderedSet[SemanticGraphNode]()

        for label in (
            # GroupByMetricLabel.get_instance(),
            MeasureLabel.get_instance(),
            TimeClusterLabel.get_instance(),
        ):
            nodes.update(self._semantic_graph.nodes_with_label(label))
        return nodes

    def _find_nearest_local_semantic_model_nodes(
        self, source_nodes: OrderedSet[SemanticGraphNode]
    ) -> OrderedSet[SemanticGraphNode]:
        candidate_target_nodes = self._semantic_graph.nodes_with_label(LocalModelLabel.get_instance())
        traversable_nodes = self._traversable_nodes_for_finding_local_semantic_model_nodes
        result = self._path_finder.find_reachable_targets(
            graph=self._semantic_graph,
            source_nodes=source_nodes,
            candidate_target_nodes=candidate_target_nodes,
            allowed_successor_nodes=traversable_nodes,
        )
        model_nodes = result.reachable_target_nodes
        if len(model_nodes) == 0:
            source_node_to_successors = {
                source_node: self._semantic_graph.successors(source_node) for source_node in source_nodes
            }
            raise RuntimeError(
                LazyFormat(
                    "Did not find any model nodes reachable from the source nodes",
                    model_nodes=model_nodes,
                    source_node_to_successors=source_node_to_successors,
                    candidate_target_nodes=candidate_target_nodes,
                    traversable_nodes=traversable_nodes,
                )
            )
        return model_nodes

    def _find_nearest_measure_nodes(self, source_nodes: OrderedSet[SemanticGraphNode]) -> FindNearestMeasureNodesResult:
        candidate_target_nodes = self._semantic_graph.nodes_with_label(MeasureLabel.get_instance())
        traversable_nodes = self._traversable_nodes_for_finding_measure_nodes
        result = self._path_finder.find_reachable_targets(
            graph=self._semantic_graph,
            source_nodes=source_nodes,
            candidate_target_nodes=candidate_target_nodes,
            allowed_successor_nodes=traversable_nodes,
            deny_labels={DenyVisibleAttributesLabel.get_instance()},
        )
        measure_nodes = result.reachable_target_nodes
        if len(measure_nodes) == 0:
            raise RuntimeError(
                LazyFormat(
                    "Did not find any measure nodes reachable from the source node.",
                    measure_nodes=measure_nodes,
                    source_nodes=source_nodes,
                    candidate_target_nodes=candidate_target_nodes,
                    traversable_nodes=traversable_nodes,
                    graph_edges=self._semantic_graph.edges,
                )
            )

        return FindNearestMeasureNodesResult(
            measure_nodes=measure_nodes,
            collected_labels=result.collected_labels,
        )

    def _remove_ambiguous_recipes(self, recipes: Sequence[AttributeQueryRecipe]) -> Sequence[AttributeQueryRecipe]:
        unique_dunder_names = set[AnyLengthTuple[str]]()
        duplicate_dunder_names = set[AnyLengthTuple[str]]()
        for recipe in recipes:
            dunder_name_elements = recipe.dunder_name_elements
            if dunder_name_elements in duplicate_dunder_names:
                continue
            if dunder_name_elements in unique_dunder_names:
                unique_dunder_names.remove(dunder_name_elements)
                duplicate_dunder_names.add(dunder_name_elements)
                continue
            unique_dunder_names.add(dunder_name_elements)

        return tuple(recipe for recipe in recipes if recipe.dunder_name_elements in unique_dunder_names)

    def _generate_specs_from_recipes(
        self, attribute_recipes: Sequence[AttributeQueryRecipe], element_filter: Optional[LinkableElementFilter]
    ) -> Sequence[AnnotatedSpec]:
        annotated_specs = tuple(mf_flatten(self._generate_spec_from_recipe(recipe) for recipe in attribute_recipes))

        if element_filter is not None:
            filtered_annotated_specs: list[AnnotatedSpec] = []
            filter_element_names = element_filter.element_names

            for annotated_spec in annotated_specs:
                properties = annotated_spec.properties
                if filter_element_names is not None and annotated_spec.spec.element_name not in filter_element_names:
                    continue
                if len(element_filter.without_any_of.intersection(properties)) > 0:
                    continue

                if len(element_filter.with_any_of.intersection(properties)) == 0:
                    continue

                without_all_of_size = len(element_filter.without_all_of)
                if 0 < without_all_of_size == len(element_filter.without_all_of.intersection(properties)):
                    continue

                filtered_annotated_specs.append(annotated_spec)

            return filtered_annotated_specs
        return annotated_specs

    def _generate_spec_from_recipe(self, recipe: AttributeQueryRecipe) -> Sequence[AnnotatedSpec]:
        element_type = mf_first_non_none_or_raise(recipe.element_type)
        dundered_name_elements = recipe.dunder_name_elements
        properties = MutableOrderedSet(recipe.properties)
        entity_link_names = tuple(element_name for element_name in recipe.dunder_name_elements[:-1])
        element_name = dundered_name_elements[-1]

        # Adjust properties to match existing behavior.
        if element_type is LinkableElementType.METRIC:
            # Group-by metrics are considered to be joined.
            properties.add(LinkableElementProperty.JOINED)
        elif (
            element_type is LinkableElementType.DIMENSION
            or element_type is LinkableElementType.TIME_DIMENSION
            or element_type is LinkableElementType.ENTITY
        ):
            pass
        else:
            assert_values_exhausted(element_type)

        # Aside from metric time, add the `LOCAL` property if only one semantic model is required to compute the
        # attribute. Otherwise, it's `JOINED`.
        model_ids = recipe.models_in_join
        model_id_count = len(model_ids)
        if model_id_count == 0:
            assert LinkableElementProperty.METRIC_TIME in properties
        elif model_id_count == 1:
            if element_type is not LinkableElementType.METRIC and LinkableElementProperty.METRIC_TIME not in properties:
                properties.add(LinkableElementProperty.LOCAL)
        elif model_id_count == 2:
            properties.add(LinkableElementProperty.JOINED)
        elif model_id_count >= 3:
            properties.update(
                (
                    LinkableElementProperty.JOINED,
                    LinkableElementProperty.MULTI_HOP,
                )
            )
        else:
            raise RuntimeError(LazyFormat("Case not handled", model_id_count=model_id_count, recipe=recipe))

        # Add `DERIVED_TIME_GRANULARITY` if the grain is different from the element's grain.
        # `metric_time` never has the `DERIVED_TIME_GRANULARITY` property.
        if (
            recipe.min_time_grain is not None
            and recipe.time_grain is not None
            and recipe.min_time_grain is not recipe.time_grain.base_granularity
        ):
            properties.add(LinkableElementProperty.DERIVED_TIME_GRANULARITY)

        last_model_id = recipe.last_model_id

        properties_tuple = tuple(properties)
        source_semantic_model_names = tuple(FrozenOrderedSet(model_id.model_name for model_id in model_ids))

        if element_type is LinkableElementType.METRIC:
            if recipe.key_query_groups is None:
                raise RuntimeError(
                    LazyFormat(
                        "Missing key-query group for a group-by-metric",
                        recipe=recipe,
                    )
                )
            return self._generate_group_by_metric_specs(
                metric_name=element_name,
                entity_link_names=entity_link_names,
                properties_tuple=properties_tuple,
                source_semantic_model_names=source_semantic_model_names,
                key_query_group=recipe.key_query_groups,
            )
        elif element_type is LinkableElementType.TIME_DIMENSION:
            element_name = dundered_name_elements[-2]
        elif (
            element_type is LinkableElementType.ENTITY
            or element_type is LinkableElementType.METRIC
            or element_type is LinkableElementType.DIMENSION
        ):
            pass
        else:
            assert_values_exhausted(element_type)

        time_grain = recipe.time_grain
        date_part = recipe.date_part
        origin_semantic_model_names = (last_model_id.model_name,) if last_model_id is not None else ()

        return (
            AnnotatedSpec(
                element_type=element_type,
                element_name=element_name,
                entity_link_names=recipe.entity_link_names,
                time_grain=time_grain,
                date_part=date_part,
                metric_subquery_entity_link_names=(),
                element_properties=properties_tuple,
                origin_semantic_model_names=origin_semantic_model_names,
                source_semantic_model_names=source_semantic_model_names,
            ),
        )

    def _generate_recipes(
        self,
        source_node: SemanticGraphNode,
        element_filter: Optional[LinkableElementFilter],
        node_allow_set: Optional[OrderedSet[SemanticGraphNode]],
        node_deny_set: Optional[OrderedSet[SemanticGraphNode]],
        max_entity_links: int = MAX_JOIN_HOPS,
    ) -> Sequence[AttributeQueryRecipe]:
        mutable_path = AttributeRecipeWriterPath.create()
        attribute_recipes = []
        target_nodes = self._semantic_graph.nodes_with_label(GroupByAttributeLabel.get_instance())
        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Generating attribute recipes",
                    source_node=source_node,
                    target_nodes=target_nodes,
                    element_filter=element_filter,
                )
            )
        for stop_event in self._path_finder.traverse_dfs(
            graph=self._semantic_graph,
            mutable_path=mutable_path,
            source_node=source_node,
            target_nodes=target_nodes,
            weight_function=DunderNameWeightFunction(
                element_name_allow_set=set(element_filter.element_names)
                if element_filter is not None and element_filter.element_names is not None
                else None,
                property_deny_set=set(element_filter.without_any_of)
                if element_filter is not None and len(element_filter.without_any_of) > 0
                else None,
            ),
            max_path_weight=max_entity_links,
            allow_node_revisits=True,
            node_allow_set=node_allow_set,
            node_deny_set=node_deny_set,
        ):
            path = stop_event.current_path
            attribute_recipe = mutable_path.recipe_writer.latest_recipe
            if self._verbose_debug_logs:
                logger.debug(
                    LazyFormat("Found path to target node", path_nodes=path.nodes, attribute_recipe=attribute_recipe)
                )

            if attribute_recipe is not None:
                attribute_recipes.append(attribute_recipe)

        return attribute_recipes

    def _generate_group_by_metric_specs(
        self,
        metric_name: str,
        entity_link_names: AnyLengthTuple[str],
        properties_tuple: AnyLengthTuple[LinkableElementProperty],
        source_semantic_model_names: AnyLengthTuple[str],
        key_query_group: KeyQueryGroup,
    ) -> Sequence[AnnotatedSpec]:
        annotated_specs = []
        # for metric_subquery in self._get_metric_subqueries_for_models(tuple(subquery_model_ids)):
        for key_query, model_ids in key_query_group.items():
            source_semantic_model_name_set = MutableOrderedSet(source_semantic_model_names)
            source_semantic_model_name_set.update(model_id.model_name for model_id in model_ids)

            annotated_specs.append(
                AnnotatedSpec(
                    element_type=LinkableElementType.METRIC,
                    entity_link_names=entity_link_names,
                    element_name=metric_name,
                    time_grain=None,
                    date_part=None,
                    metric_subquery_entity_link_names=key_query,
                    element_properties=properties_tuple,
                    origin_semantic_model_names=(),
                    source_semantic_model_names=tuple(source_semantic_model_name_set),
                )
            )

        return annotated_specs

    @cached_property
    def _traversable_nodes_for_finding_metric_subqueries(self) -> OrderedSet[SemanticGraphNode]:
        nodes = MutableOrderedSet[SemanticGraphNode]()

        for label in (
            JoinedModelLabel.get_instance(),
            LocalModelLabel.get_instance(),
            ConfiguredEntityLabel.get_instance(),
            KeyEntityClusterLabel.get_instance(),
        ):
            nodes.update(self._semantic_graph.nodes_with_label(label))
        return nodes


@dataclass
class FindNearestMeasureNodesResult:
    measure_nodes: OrderedSet[SemanticGraphNode]
    collected_labels: OrderedSet[MetricflowGraphLabel]


# @fast_frozen_dataclass()
# class ElementSetCacheKey:
#     node: SemanticGraphNode
#     element_filter: Optional[LinkableElementFilter]


@fast_frozen_dataclass()
class ElementSetCacheKey:
    nodes: FrozenOrderedSet[SemanticGraphNode]
    element_filter: Optional[LinkableElementFilter]
