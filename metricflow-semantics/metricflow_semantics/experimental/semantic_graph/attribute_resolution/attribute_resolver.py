from __future__ import annotations

import logging
from functools import cached_property
from typing import Optional, Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.references import EntityReference, SemanticModelReference

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.collection_helpers.syntactic_sugar import mf_first_non_none_or_raise
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import AttributeRecipe
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.experimental.semantic_graph.builder.dunder_name_weight import DunderNameWeightFunction
from metricflow_semantics.experimental.semantic_graph.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import LocalModelNode
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    DsiEntityLabel,
    GroupByAttributeLabel,
    JoinedModelLabel,
    KeyEntityClusterLabel,
    LocalModelLabel,
    MeasureAttributeLabel,
    MetricAttributeLabel,
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
        self._verbose_debug_logs = True
        self._semantic_model_id_to_metric_subqueries: dict[SemanticModelId, OrderedSet[MetricSubquery]] = {}

    def _generate_non_metric_time_recipes(self, model_node: SemanticGraphNode) -> Sequence[AttributeRecipe]:
        return self._remove_ambiguous_recipes(self._generate_recipes(model_node))

    def _generate_metric_time_recipes(self, measure_node: SemanticGraphNode) -> Sequence[AttributeRecipe]:
        return self._remove_ambiguous_recipes(
            self._generate_recipes(
                source_node=measure_node,
                traversable_nodes=self._traversable_nodes_for_metric_time_recipes,
            )
        )

    def resolve_annotated_specs(self, source_node: SemanticGraphNode) -> Sequence[AnnotatedSpec]:
        measure_node = self._find_nearest_measure_node(source_node)
        model_node = self._find_nearest_local_semantic_model_node(measure_node)

        non_metric_time_recipes = tuple(self._generate_non_metric_time_recipes(model_node))
        # non_metric_time_recipes = ()
        metric_time_recipes = tuple(self._generate_metric_time_recipes(measure_node))
        all_recipes = metric_time_recipes + non_metric_time_recipes
        logger.debug(
            LazyFormat(
                "Generated attribute recipes",
                all_recipes=all_recipes,
            )
        )
        return self._generate_specs_from_recipes(all_recipes)

    @cached_property
    def _traversable_nodes_for_finding_measure_nodes(self) -> OrderedSet[SemanticGraphNode]:
        nodes = MutableOrderedSet[SemanticGraphNode]()

        for label in (
            MetricAttributeLabel.get_instance(),
            MeasureAttributeLabel.get_instance(),
        ):
            nodes.update(self._semantic_graph.nodes_with_label(label))
        return nodes

    @cached_property
    def _traversable_nodes_for_finding_local_semantic_model_nodes(self) -> OrderedSet[SemanticGraphNode]:
        nodes = MutableOrderedSet[SemanticGraphNode]()

        for label in (
            MetricAttributeLabel.get_instance(),
            MeasureAttributeLabel.get_instance(),
            LocalModelLabel.get_instance(),
        ):
            nodes.update(self._semantic_graph.nodes_with_label(label))
        return nodes

    @cached_property
    def _traversable_nodes_for_metric_time_recipes(self) -> OrderedSet[SemanticGraphNode]:
        nodes = MutableOrderedSet[SemanticGraphNode]()

        for label in (
            MetricAttributeLabel.get_instance(),
            MeasureAttributeLabel.get_instance(),
            TimeClusterLabel.get_instance(),
        ):
            nodes.update(self._semantic_graph.nodes_with_label(label))
        return nodes

    def _find_nearest_local_semantic_model_node(self, source_node: SemanticGraphNode) -> SemanticGraphNode:
        candidate_target_nodes = self._semantic_graph.nodes_with_label(LocalModelLabel.get_instance())
        traversable_nodes = self._traversable_nodes_for_finding_local_semantic_model_nodes
        model_nodes = tuple(
            self._path_finder.find_reachable_targets(
                graph=self._semantic_graph,
                source_nodes=FrozenOrderedSet((source_node,)),
                candidate_target_nodes=candidate_target_nodes,
                traversable_nodes=traversable_nodes,
            )
        )
        if len(model_nodes) != 1:
            raise RuntimeError(
                LazyFormat(
                    "Did not find exactly one local-semantic-model node",
                    source_node=source_node,
                    model_nodes=model_nodes,
                    candidate_target_nodes=candidate_target_nodes,
                    traversable_nodes=traversable_nodes,
                )
            )
        return model_nodes[0]

    def _find_nearest_measure_node(self, source_node: SemanticGraphNode) -> SemanticGraphNode:
        candidate_target_nodes = self._semantic_graph.nodes_with_label(MeasureAttributeLabel.get_instance())
        traversable_nodes = self._traversable_nodes_for_finding_measure_nodes
        measure_nodes = tuple(
            self._path_finder.find_reachable_targets(
                graph=self._semantic_graph,
                source_nodes=FrozenOrderedSet((source_node,)),
                candidate_target_nodes=candidate_target_nodes,
                traversable_nodes=traversable_nodes,
            )
        )
        if len(measure_nodes) != 1:
            raise RuntimeError(
                LazyFormat(
                    "Did not find exactly one measure node",
                    source_node=source_node,
                    measure_nodes=measure_nodes,
                    candidate_target_nodes=candidate_target_nodes,
                    traversable_nodes=traversable_nodes,
                )
            )
        return measure_nodes[0]

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

    def _generate_specs_from_recipes(self, attribute_recipes: Sequence[AttributeRecipe]) -> Sequence[AnnotatedSpec]:
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
                raise RuntimeError(
                    LazyFormat(
                        "An attributed descriptor must be derived from at least one model.",
                        descriptor=recipe,
                        model_ids=model_ids,
                    )
                )
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
                raise RuntimeError(LazyFormat("Case not handled", model_id_count=model_id_count))

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
            # `last_model_id` may be `None` if the descriptor is for a `metric_time` attribute since the path from
            # the `GroupByAttributeRoot` purposefully excludes that to retain parallelism.

            if last_model_id is None:
                raise NotImplementedError
            origin_model_ids = FrozenOrderedSet((last_model_id,))

            if element_type is LinkableElementType.METRIC:
                dunder_name_to_annotated_spec.update(
                    self._generate_group_by_metric_specs(
                        metric_name=default_element_name,
                        entity_links=default_entity_links,
                        properties=properties,
                        source_models=derived_from_semantic_models,
                        subquery_model_ids=recipe.subquery_model_ids,
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
                    properties=properties,
                    origin_model_ids=origin_model_ids,
                    derived_from_semantic_models=derived_from_semantic_models,
                )
            elif element_type is LinkableElementType.TIME_DIMENSION:
                element_name = dundered_name_elements[-2]
                entity_links = tuple(
                    EntityReference(element_name=element_name) for element_name in recipe.dunder_name_elements[:-2]
                )
                time_grain: Optional[ExpandedTimeGranularity]
                time_dimension_spec = TimeDimensionSpec(
                    element_name=element_name,
                    entity_links=entity_links,
                    time_granularity=recipe.time_grain,
                    date_part=recipe.date_part,
                )
                dunder_name = time_dimension_spec.qualified_name
                existing_annotated_spec = dunder_name_to_annotated_spec.get(dunder_name)

                if existing_annotated_spec is None or (
                    LinkableElementProperty.DERIVED_TIME_GRANULARITY in existing_annotated_spec.properties
                    and LinkableElementProperty.DERIVED_TIME_GRANULARITY not in properties
                ):
                    dunder_name_to_annotated_spec[dunder_name] = AnnotatedSpec.create(
                        element_type=element_type,
                        spec=time_dimension_spec,
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
                    properties=properties,
                    origin_model_ids=origin_model_ids,
                    derived_from_semantic_models=derived_from_semantic_models,
                )
            else:
                assert_values_exhausted(element_type)

        return tuple(dunder_name_to_annotated_spec.values())

    def _generate_recipes(
        self,
        source_node: SemanticGraphNode,
        max_entity_links: int = MAX_JOIN_HOPS,
        traversable_nodes: Optional[OrderedSet[SemanticGraphNode]] = None,
    ) -> Sequence[AttributeRecipe]:
        mutable_path = AttributeRecipeWriterPath.create()
        attribute_recipes = []
        target_nodes = self._semantic_graph.nodes_with_label(GroupByAttributeLabel.get_instance())
        logger.debug(
            LazyFormat(
                "Generating attribute recipes",
                source_node=source_node,
                target_nodes=target_nodes,
            )
        )
        for stop_event in self._path_finder.traverse_dfs(
            graph=self._semantic_graph,
            mutable_path=mutable_path,
            source_node=source_node,
            target_nodes=target_nodes,
            weight_function=DunderNameWeightFunction(),
            max_path_weight=max_entity_links,
            allow_node_revisits=True,
            traversable_nodes=traversable_nodes,
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

    def _get_metric_subqueries_for_model(self, model_id: SemanticModelId) -> OrderedSet[MetricSubquery]:
        metric_subqueries = self._semantic_model_id_to_metric_subqueries.get(model_id)
        if metric_subqueries is not None:
            return metric_subqueries

        attribute_recipes = self._generate_recipes(
            source_node=LocalModelNode.get_instance(model_id),
            max_entity_links=1,
            traversable_nodes=self._traversable_nodes_for_finding_metric_subqueries,
        )

        attribute_recipes = self._remove_ambiguous_recipes(attribute_recipes)

        logger.debug(
            LazyFormat("Got recipes for metric subqueries", model_id=model_id, attribute_recipes=attribute_recipes)
        )

        metric_subqueries = MutableOrderedSet[MetricSubquery]()

        for recipe in attribute_recipes:
            metric_subqueries.add(
                MetricSubquery(
                    metric_subquery_elements=recipe.dunder_name_elements,
                    source_model_ids=FrozenOrderedSet(recipe.models_in_join),
                )
            )

        logger.debug(
            LazyFormat(
                "Generated metric subqueries",
                metric_subqueries=metric_subqueries,
            )
        )

        return metric_subqueries

    def _get_metric_subqueries_for_models(self, model_ids: Sequence[SemanticModelId]) -> Sequence[MetricSubquery]:
        metric_subqueries: Optional[OrderedSet[MetricSubquery]] = None

        metric_subquery_key_to_model_ids: dict[AnyLengthTuple[str], MutableOrderedSet[SemanticModelId]] = {}
        keys_to_exclude: set[AnyLengthTuple[str]] = set()

        for model_id in model_ids:
            if metric_subqueries is None:
                metric_subqueries = self._get_metric_subqueries_for_model(model_id)
                for metric_subquery in metric_subqueries:
                    metric_subquery_key_to_model_ids[metric_subquery.metric_subquery_elements] = MutableOrderedSet(
                        metric_subquery.source_model_ids
                    )
            else:
                other_subqueries = self._get_metric_subqueries_for_model(model_id)
                for metric_subquery in other_subqueries:
                    if metric_subquery.metric_subquery_elements not in metric_subquery_key_to_model_ids:
                        keys_to_exclude.add(metric_subquery.metric_subquery_elements)
                    else:
                        metric_subquery_key_to_model_ids[metric_subquery.metric_subquery_elements].update(
                            metric_subquery.source_model_ids
                        )

        if metric_subqueries is None:
            return ()

        return tuple(
            MetricSubquery(metric_subquery_elements=subquery_elements, source_model_ids=FrozenOrderedSet(model_ids))
            for subquery_elements, model_ids in metric_subquery_key_to_model_ids.items()
            if subquery_elements not in keys_to_exclude
        )

    def _generate_group_by_metric_specs(
        self,
        metric_name: str,
        entity_links: Sequence[EntityReference],
        properties: OrderedSet[LinkableElementProperty],
        source_models: OrderedSet[SemanticModelReference],
        subquery_model_ids: OrderedSet[SemanticModelId],
    ) -> dict[str, AnnotatedSpec]:
        dunder_name_to_annotated_spec: dict[str, AnnotatedSpec] = {}

        for metric_subquery in self._get_metric_subqueries_for_models(tuple(subquery_model_ids)):
            if entity_links[-1].element_name != metric_subquery.metric_subquery_elements[-1]:
                continue
            spec = GroupByMetricSpec(
                element_name=metric_name,
                entity_links=tuple(entity_links),
                metric_subquery_entity_links=tuple(
                    EntityReference(element_name) for element_name in metric_subquery.metric_subquery_elements
                ),
            )

            dunder_name_to_annotated_spec[spec.qualified_name] = AnnotatedSpec.create(
                element_type=LinkableElementType.METRIC,
                spec=spec,
                properties=properties,
                origin_model_ids=(
                    SemanticModelId(
                        model_name=SemanticModelDerivation.VIRTUAL_SEMANTIC_MODEL_REFERENCE.semantic_model_name
                    ),
                ),
                derived_from_semantic_models=source_models.union(
                    model_id.semantic_model_reference for model_id in metric_subquery.source_model_ids
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


@fast_frozen_dataclass()
class MetricSubquery:
    metric_subquery_elements: AnyLengthTuple[str]
    source_model_ids: FrozenOrderedSet[SemanticModelId]
