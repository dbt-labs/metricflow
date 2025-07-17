from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import cached_property
from typing import Optional, Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.references import EntityReference, SemanticModelReference

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.collection_helpers.syntactic_sugar import mf_first_item, mf_first_non_none_or_raise
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.metricflow_exception import MetricflowInternalError
from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import AttributeRecipe
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.annotated_spec_linkable_element_set import (
    AnnotatedSpecLinkableElementSet,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.key_query_set import (
    DsiEntityKeyQueryGroup,
)
from metricflow_semantics.experimental.semantic_graph.builder.dunder_name_weight import DunderNameWeightFunction
from metricflow_semantics.experimental.semantic_graph.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    CumulativeMeasureLabel,
    DenyDatePartLabel,
    DenyVisibleAttributesLabel,
    DsiEntityLabel,
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
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder import (
    MetricflowGraphPathFinder,
)
from metricflow_semantics.experimental.semantic_graph.semantic_graph import SemanticGraph
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantic_model_derivation import SemanticModelDerivation
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.model.semantics.linkable_element_set_base import AnnotatedSpec
from metricflow_semantics.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

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

        self._model_node_to_non_metric_time_recipes: dict[ElementSetCacheKey, AnnotatedSpecLinkableElementSet] = {}
        self._measure_node_to_metric_time_recipes: dict[ElementSetCacheKey, AnnotatedSpecLinkableElementSet] = {}

    def _generate_non_metric_time_recipes(
        self, model_node: SemanticGraphNode, element_filter: Optional[LinkableElementFilter]
    ) -> Sequence[AttributeRecipe]:
        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Generating non-metric-time recipes",
                    model_node=model_node,
                )
            )
        return self._remove_ambiguous_recipes(
            self._generate_recipes(model_node, node_allow_list=None, element_filter=element_filter)
        )

    def _generate_metric_time_recipes(
        self, measure_node: SemanticGraphNode, element_filter: Optional[LinkableElementFilter]
    ) -> Sequence[AttributeRecipe]:
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
                node_allow_list=allowed_nodes,
                element_filter=element_filter,
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
                    node_allow_list=self._semantic_graph.nodes_with_label(TimeClusterLabel.get_instance()),
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
            cache_key = ElementSetCacheKey(node=model_node, element_filter=element_filter)
            element_set = self._model_node_to_non_metric_time_recipes.get(cache_key)
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

            self._model_node_to_non_metric_time_recipes[cache_key] = element_set

        metric_time_elements_sets_to_intersect: list[AnnotatedSpecLinkableElementSet] = []
        for measure_node in measure_nodes:
            cache_key = ElementSetCacheKey(node=measure_node, element_filter=element_filter)
            element_set = self._measure_node_to_metric_time_recipes.get(cache_key)
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
            self._measure_node_to_metric_time_recipes[cache_key] = element_set
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

        return model_node_element_set.union(metric_time_element_set)

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

    def _remove_ambiguous_recipes(self, recipes: Sequence[AttributeRecipe]) -> Sequence[AttributeRecipe]:
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
        self, attribute_recipes: Sequence[AttributeRecipe], element_filter: Optional[LinkableElementFilter]
    ) -> Sequence[AnnotatedSpec]:
        dunder_name_to_annotated_spec: dict[str, AnnotatedSpec] = {}
        for recipe in attribute_recipes:
            element_type = mf_first_non_none_or_raise(recipe.element_type)
            dundered_name_elements = recipe.dunder_name_elements
            properties = MutableOrderedSet(recipe.properties)
            default_entity_links = tuple(
                EntityReference(element_name=element_name) for element_name in recipe.dunder_name_elements[:-1]
            )
            default_element_name = dundered_name_elements[-1]

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
                if (
                    element_type is not LinkableElementType.METRIC
                    and LinkableElementProperty.METRIC_TIME not in properties
                ):
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

            derived_from_semantic_models = FrozenOrderedSet(
                SemanticModelReference(semantic_model_name=model_id.model_name) for model_id in model_ids
            )
            last_model_id = recipe.last_model_id

            # `last_model_id` may be `None` if the descriptor is for a `metric_time` attribute
            origin_model_ids: FrozenOrderedSet[SemanticModelId] = (
                FrozenOrderedSet((last_model_id,)) if last_model_id is not None else FrozenOrderedSet()
            )

            if element_type is LinkableElementType.METRIC:
                if recipe.key_query_set is None:
                    raise RuntimeError(
                        LazyFormat(
                            "Missing key-query set to generate a group-by-metric spec",
                            recipe=recipe,
                        )
                    )
                dunder_name_to_annotated_spec.update(
                    self._generate_group_by_metric_specs(
                        metric_name=default_element_name,
                        entity_links=default_entity_links,
                        properties=properties,
                        source_models=derived_from_semantic_models,
                        key_query_group=recipe.key_query_set,
                    )
                )
            elif element_type is LinkableElementType.ENTITY:
                entity_spec = EntitySpec(
                    element_name=default_element_name,
                    entity_links=default_entity_links,
                )
                dunder_name_to_annotated_spec[entity_spec.qualified_name] = AnnotatedSpec.create(
                    element_type=element_type,
                    spec=entity_spec,
                    time_grain=None,
                    date_part=None,
                    properties=properties,
                    origin_model_ids=origin_model_ids,
                    derived_from_semantic_models=derived_from_semantic_models,
                )
            elif element_type is LinkableElementType.TIME_DIMENSION:
                element_name = dundered_name_elements[-2]
                entity_links = tuple(
                    EntityReference(element_name=element_name) for element_name in recipe.dunder_name_elements[:-2]
                )
                time_grain: Optional[ExpandedTimeGranularity] = recipe.time_grain
                time_dimension_spec = TimeDimensionSpec(
                    element_name=element_name,
                    entity_links=entity_links,
                    time_granularity=time_grain,
                    date_part=recipe.date_part,
                )
                dunder_name = time_dimension_spec.qualified_name
                dunder_name_to_annotated_spec[dunder_name] = AnnotatedSpec.create(
                    element_type=element_type,
                    spec=time_dimension_spec,
                    time_grain=time_grain,
                    date_part=recipe.date_part,
                    properties=properties,
                    origin_model_ids=origin_model_ids,
                    derived_from_semantic_models=derived_from_semantic_models,
                )
            elif element_type is LinkableElementType.DIMENSION:
                spec = DimensionSpec(
                    element_name=default_element_name,
                    entity_links=default_entity_links,
                )

                dunder_name_to_annotated_spec[spec.qualified_name] = AnnotatedSpec.create(
                    element_type=element_type,
                    spec=spec,
                    time_grain=None,
                    date_part=None,
                    properties=properties,
                    origin_model_ids=origin_model_ids,
                    derived_from_semantic_models=derived_from_semantic_models,
                )
            else:
                assert_values_exhausted(element_type)

        if element_filter is not None:
            annotated_specs_to_return: list[AnnotatedSpec] = []
            filter_element_names = element_filter.element_names

            for annotated_spec in dunder_name_to_annotated_spec.values():
                if filter_element_names is not None and annotated_spec.spec.element_name not in filter_element_names:
                    continue
                if len(element_filter.without_any_of.intersection(annotated_spec.properties)) > 0:
                    continue

                if len(element_filter.with_any_of.intersection(annotated_spec.properties)) == 0:
                    continue

                without_all_of_size = len(element_filter.without_all_of)
                if 0 < without_all_of_size == len(element_filter.without_all_of.intersection(properties)):
                    continue

                annotated_specs_to_return.append(annotated_spec)

            return annotated_specs_to_return

        return tuple(dunder_name_to_annotated_spec.values())

    def _generate_recipes(
        self,
        source_node: SemanticGraphNode,
        element_filter: Optional[LinkableElementFilter],
        node_allow_list: Optional[OrderedSet[SemanticGraphNode]],
        max_entity_links: int = MAX_JOIN_HOPS,
    ) -> Sequence[AttributeRecipe]:
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
            allowed_nodes=node_allow_list,
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
        entity_links: Sequence[EntityReference],
        properties: OrderedSet[LinkableElementProperty],
        source_models: OrderedSet[SemanticModelReference],
        key_query_group: DsiEntityKeyQueryGroup,
    ) -> dict[str, AnnotatedSpec]:
        dunder_name_to_annotated_spec: dict[str, AnnotatedSpec] = {}

        # for metric_subquery in self._get_metric_subqueries_for_models(tuple(subquery_model_ids)):
        for key_query, model_ids in key_query_group.items():
            # if entity_links[-1].element_name != key_query[-1]:
            #     continue

            spec = GroupByMetricSpec(
                element_name=metric_name,
                entity_links=tuple(entity_links),
                metric_subquery_entity_links=tuple(EntityReference(element_name) for element_name in key_query),
            )

            dunder_name_to_annotated_spec[spec.qualified_name] = AnnotatedSpec.create(
                element_type=LinkableElementType.METRIC,
                spec=spec,
                time_grain=None,
                date_part=None,
                properties=properties,
                origin_model_ids=(
                    SemanticModelId(
                        model_name=SemanticModelDerivation.VIRTUAL_SEMANTIC_MODEL_REFERENCE.semantic_model_name
                    ),
                ),
                derived_from_semantic_models=source_models.union(
                    model_id.semantic_model_reference for model_id in model_ids
                ),
            )
        return dunder_name_to_annotated_spec

    @cached_property
    def _traversable_nodes_for_finding_metric_subqueries(self) -> OrderedSet[SemanticGraphNode]:
        nodes = MutableOrderedSet[SemanticGraphNode]()

        for label in (
            JoinedModelLabel.get_instance(),
            LocalModelLabel.get_instance(),
            DsiEntityLabel.get_instance(),
            KeyEntityClusterLabel.get_instance(),
        ):
            nodes.update(self._semantic_graph.nodes_with_label(label))
        return nodes


@dataclass
class FindNearestMeasureNodesResult:
    measure_nodes: OrderedSet[SemanticGraphNode]
    collected_labels: OrderedSet[MetricflowGraphLabel]


@fast_frozen_dataclass()
class ElementSetCacheKey:
    node: SemanticGraphNode
    element_filter: Optional[LinkableElementFilter]
