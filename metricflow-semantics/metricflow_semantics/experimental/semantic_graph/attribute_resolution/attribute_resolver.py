from __future__ import annotations

import dataclasses
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Optional

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.references import EntityReference, SemanticModelReference

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.collection_helpers.syntactic_sugar import mf_first_item, mf_flatten
from metricflow_semantics.experimental.cache.mf_cache import MetricflowCache, WeakValueDictCache
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.metricflow_exception import MetricflowAssertionError
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, OrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_computation import (
    AttributeComputationUpdate,
    AttributeDescriptor,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeComputationPath,
)
from metricflow_semantics.experimental.semantic_graph.builder.dunder_name_weight import DunderNameWeightFunction
from metricflow_semantics.experimental.semantic_graph.builder.group_by_attribute_subgraph import (
    GroupByAttributeSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import (
    DsiEntityKeyAttributeNode,
    MetricNode,
)
from metricflow_semantics.experimental.semantic_graph.nodes.entity_node import GroupByAttributeRootNode
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    GroupByAttributeLabel,
    MetricAttributeLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder import (
    MetricflowGraphPathFinder,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder_cache import PathFinderCache
from metricflow_semantics.experimental.semantic_graph.semantic_graph import SemanticGraph
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantic_model_derivation import SemanticModelDerivation
from metricflow_semantics.model.semantics.linkable_element import (
    LinkableElementType,
)
from metricflow_semantics.model.semantics.linkable_element_set_base import AnnotatedSpec
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
        attribute_resolver_cache: AttributeResolverCache,
    ) -> None:
        self._manifest_object_lookup = manifest_object_lookup
        self._semantic_graph = semantic_graph
        self._path_finder = MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeComputationPath](
            path_finder_cache=PathFinderCache()
        )
        self._attribute_resolver_cache = attribute_resolver_cache
        self._group_by_attribute_subgraph_generator = GroupByAttributeSubgraphGenerator(
            semantic_graph=self._semantic_graph,
            path_finder=self._path_finder,
        )
        self._verbose_debug_logs = True

    @property
    def semantic_graph(self) -> SemanticGraph:
        return self._semantic_graph

    def _resolve_descriptors_from_source_nodes(
        self,
        source_nodes: OrderedSet[SemanticGraphNode],
        target_attribute_nodes: OrderedSet[SemanticGraphNode],
    ) -> AttributeDescriptorResult:
        subgraph_generator_result = self._group_by_attribute_subgraph_generator.generate_subgraph(
            source_nodes=source_nodes,
        )
        group_by_attribute_subgraph = subgraph_generator_result.subgraph
        mutable_path = AttributeComputationPath.create()
        attribute_descriptors = []

        for stop_event in self._path_finder.traverse_dfs(
            graph=group_by_attribute_subgraph,
            mutable_path=mutable_path,
            source_node=GroupByAttributeRootNode(),
            target_nodes=target_attribute_nodes,
            weight_function=DunderNameWeightFunction(),
            max_path_weight=DunderNameWeightFunction.MAX_ENTITY_LINKS,
            allow_node_revisits=True,
        ):
            path = stop_event.current_path
            attribute_descriptor = mutable_path.attribute_computation.attribute_descriptor
            logger.debug(LazyFormat("Got path", path_nodes=path.nodes, descriptor=attribute_descriptor))
            if attribute_descriptor is not None:
                attribute_descriptors.append(attribute_descriptor)

        return AttributeDescriptorResult(
            measure_model_ids=subgraph_generator_result.additional_derivative_model_ids,
            attribute_computation_updates=subgraph_generator_result.attribute_computation_updates,
            attribute_descriptors=tuple(attribute_descriptors),
        )

    def resolve_descriptors_for_metric(self, metric_name: str) -> AttributeDescriptorResult:
        matching_nodes = self._semantic_graph.nodes_with_label(MetricAttributeLabel(metric_name=metric_name))
        metric_node = mf_first_item(
            matching_nodes,
            lambda: MetricflowAssertionError(
                LazyFormat(
                    "Did not find exactly 1 node in the semantic graph with the given metric name",
                    metric_name=metric_name,
                    matching_nodes=matching_nodes,
                )
            ),
        )
        attribute_nodes = self._semantic_graph.nodes_with_label(GroupByAttributeLabel())
        return self._resolve_descriptors_from_source_nodes(
            source_nodes=FrozenOrderedSet((metric_node,)),
            target_attribute_nodes=attribute_nodes,
        )

    def resolve_specs_from_source_node(self, source_node: SemanticGraphNode) -> FrozenOrderedSet[AnnotatedSpec]:
        annotated_specs = list[AnnotatedSpec]()
        attribute_nodes = self._semantic_graph.nodes_with_label(GroupByAttributeLabel())
        descriptor_result = self._resolve_descriptors_from_source_nodes(
            source_nodes=FrozenOrderedSet((source_node,)),
            target_attribute_nodes=attribute_nodes,
        )

        if self._verbose_debug_logs:
            logger.debug(
                LazyFormat(
                    "Resolved descriptors",
                    source_node=source_node,
                    descriptor_result=descriptor_result,
                )
            )

        base_properties = FrozenOrderedSet(
            mf_flatten(
                update.linkable_element_property_additions for update in descriptor_result.attribute_computation_updates
            )
        )

        for descriptor in descriptor_result.attribute_descriptors:
            if len(descriptor.element_types) != 1:
                raise ValueError(
                    LazyFormat(
                        "Expected exactly one element type",
                        element_types=descriptor.element_types,
                        descriptor=descriptor,
                    )
                )
            element_type = mf_first_item(descriptor.element_types)
            dundered_name_elements = descriptor.dundered_name_elements
            properties = base_properties.union(descriptor.properties)
            default_entity_links = tuple(
                EntityReference(element_name=element_name) for element_name in descriptor.dundered_name_elements[:-1]
            )
            default_element_name = dundered_name_elements[-1]
            measure_model_ids = descriptor_result.measure_model_ids
            model_ids = measure_model_ids.union(descriptor.model_ids)

            # Adjust properties to match existing behavior.
            if element_type is LinkableElementType.METRIC:
                # Group-by metrics are considered to be joined.
                properties = properties.union((LinkableElementProperty.JOINED,))
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
            model_id_count = len(model_ids)
            if model_id_count == 0:
                raise RuntimeError(
                    LazyFormat(
                        "An attributed descriptor must be derived from at least one model.",
                        descriptor=descriptor,
                        model_ids=model_ids,
                    )
                )
            elif model_id_count == 1:
                if (
                    LinkableElementProperty.METRIC_TIME not in properties
                    and LinkableElementProperty.METRIC not in properties
                ):
                    properties = properties.union((LinkableElementProperty.LOCAL,))
            elif model_id_count == 2:
                properties = properties.union((LinkableElementProperty.JOINED,))
            elif model_id_count >= 3:
                properties = properties.union(
                    (
                        LinkableElementProperty.JOINED,
                        LinkableElementProperty.MULTI_HOP,
                    )
                )
            else:
                raise RuntimeError(LazyFormat("Case not handled", model_id_count=model_id_count))

            derived_from_semantic_models = FrozenOrderedSet(
                SemanticModelReference(semantic_model_name=model_id.model_name) for model_id in model_ids
            )
            last_model_id = descriptor.last_model_id
            # `last_model_id` may be `None` if the descriptor is for a `metric_time` attribute since the path from
            # the `GroupByAttributeRoot` purposefully excludes that to retain parallelism.
            origin_model_ids = FrozenOrderedSet((last_model_id,)) if last_model_id is not None else measure_model_ids
            if element_type is LinkableElementType.METRIC:
                annotated_specs.extend(
                    self._generate_group_by_metric_specs(
                        metric_name=default_element_name,
                        entity_links=default_entity_links,
                        properties=properties,
                        derived_from_semantic_models=derived_from_semantic_models,
                    )
                )
            elif element_type is LinkableElementType.ENTITY:
                annotated_specs.append(
                    AnnotatedSpec.create(
                        element_type=element_type,
                        spec=EntitySpec(
                            element_name=default_element_name,
                            entity_links=default_entity_links,
                        ),
                        properties=properties,
                        origin_model_ids=origin_model_ids,
                        derived_from_semantic_models=derived_from_semantic_models,
                    )
                )
            elif element_type is LinkableElementType.TIME_DIMENSION:
                element_name = dundered_name_elements[-2]
                entity_links = tuple(
                    EntityReference(element_name=element_name)
                    for element_name in descriptor.dundered_name_elements[:-2]
                )
                time_grain: Optional[ExpandedTimeGranularity]
                annotated_specs.append(
                    AnnotatedSpec.create(
                        element_type=element_type,
                        spec=TimeDimensionSpec(
                            element_name=element_name,
                            entity_links=entity_links,
                            time_granularity=mf_first_item(descriptor.time_grains)
                            if len(descriptor.time_grains) > 0
                            else None,
                            date_part=mf_first_item(descriptor.date_parts) if len(descriptor.date_parts) > 0 else None,
                        ),
                        properties=properties,
                        origin_model_ids=origin_model_ids,
                        derived_from_semantic_models=derived_from_semantic_models,
                    )
                )
            elif element_type is LinkableElementType.DIMENSION:
                annotated_specs.append(
                    AnnotatedSpec.create(
                        element_type=element_type,
                        spec=DimensionSpec(
                            element_name=default_element_name,
                            entity_links=default_entity_links,
                        ),
                        properties=properties,
                        origin_model_ids=origin_model_ids,
                        derived_from_semantic_models=derived_from_semantic_models,
                    )
                )
            else:
                assert_values_exhausted(element_type)
        return FrozenOrderedSet(annotated_specs)

    def _generate_group_by_metric_specs(
        self,
        metric_name: str,
        entity_links: Sequence[EntityReference],
        properties: FrozenOrderedSet[LinkableElementProperty],
        derived_from_semantic_models: FrozenOrderedSet[SemanticModelReference],
    ) -> AnyLengthTuple[AnnotatedSpec]:
        group_by_metric_specs: list[AnnotatedSpec] = []
        metric_node = MetricNode(attribute_name=metric_name)
        entity_value_node = DsiEntityKeyAttributeNode(attribute_name=entity_links[-1].element_name)

        result = self._resolve_descriptors_from_source_nodes(
            source_nodes=FrozenOrderedSet((metric_node,)),
            target_attribute_nodes=FrozenOrderedSet((entity_value_node,)),
        )

        for descriptor in result.attribute_descriptors:
            group_by_metric_specs.append(
                AnnotatedSpec.create(
                    element_type=LinkableElementType.METRIC,
                    spec=GroupByMetricSpec(
                        element_name=metric_name,
                        entity_links=tuple(entity_links),
                        metric_subquery_entity_links=tuple(
                            EntityReference(element_name=element) for element in descriptor.dundered_name_elements
                        ),
                    ),
                    properties=properties,
                    origin_model_ids=(
                        SemanticModelId(
                            model_name=SemanticModelDerivation.VIRTUAL_SEMANTIC_MODEL_REFERENCE.semantic_model_name
                        ),
                    ),
                    derived_from_semantic_models=derived_from_semantic_models,
                )
            )
        return tuple(group_by_metric_specs)


@singleton_dataclass()
class MetricSubqueryPattern:
    metric_name: str
    last_entity_link: str


@dataclass
class AttributeResolverCache(MetricflowCache):
    metric_name_to_attribute_descriptors: WeakValueDictCache[
        str, AnyLengthTuple[AttributeDescriptor]
    ] = dataclasses.field(default_factory=WeakValueDictCache)
    metric_subquery_to_subquery_entity_links: WeakValueDictCache[
        MetricSubqueryPattern, AnyLengthTuple[AnyLengthTuple[str]]
    ] = dataclasses.field(default_factory=WeakValueDictCache)
    measure_node_to_attribute_descriptors: WeakValueDictCache[
        SemanticGraphNode, AnyLengthTuple[AttributeDescriptor]
    ] = dataclasses.field(default_factory=WeakValueDictCache)


@fast_frozen_dataclass()
class AttributeDescriptorResult:
    measure_model_ids: FrozenOrderedSet[SemanticModelId]
    attribute_computation_updates: AnyLengthTuple[AttributeComputationUpdate]
    attribute_descriptors: AnyLengthTuple[AttributeDescriptor]
