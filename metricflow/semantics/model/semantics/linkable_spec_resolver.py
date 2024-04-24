from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, FrozenSet, List, Optional, Sequence, Set, Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols.dimension import Dimension, DimensionType
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.protocols.semantic_model import SemanticModel
from dbt_semantic_interfaces.references import (
    MeasureReference,
    MetricReference,
    SemanticModelReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums import MetricType
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from dbt_semantic_interfaces.validations.unique_valid_name import MetricFlowReservedKeywords

from metricflow.semantics.errors.error_classes import UnknownMetricLinkingError
from metricflow.semantics.mf_logging.pretty_print import mf_pformat
from metricflow.semantics.model.semantics.linkable_element import (
    ElementPathKey,
    LinkableDimension,
    LinkableElementProperty,
    LinkableElementType,
    LinkableEntity,
    LinkableMetric,
    SemanticModelJoinPath,
    SemanticModelJoinPathElement,
)
from metricflow.semantics.model.semantics.linkable_element_set import LinkableElementSet
from metricflow.semantics.model.semantics.semantic_model_join_evaluator import SemanticModelJoinEvaluator
from metricflow.semantics.specs.spec_classes import (
    DEFAULT_TIME_GRANULARITY,
    EntityReference,
)

if TYPE_CHECKING:
    from metricflow.semantics.model.semantics.semantic_model_lookup import SemanticModelLookup


logger = logging.getLogger(__name__)


def _generate_linkable_time_dimensions(
    semantic_model_origin: SemanticModelReference,
    dimension: Dimension,
    entity_links: Tuple[EntityReference, ...],
    join_path: Sequence[SemanticModelJoinPathElement],
    with_properties: FrozenSet[LinkableElementProperty],
) -> Sequence[LinkableDimension]:
    """Generates different versions of the given dimension, but at other valid time granularities."""
    linkable_dimensions = []

    defined_time_granularity = (
        dimension.type_params.time_granularity if dimension.type_params else DEFAULT_TIME_GRANULARITY
    )
    for time_granularity in TimeGranularity:
        if time_granularity < defined_time_granularity:
            continue
        properties = set(with_properties)
        if time_granularity != defined_time_granularity:
            properties.add(LinkableElementProperty.DERIVED_TIME_GRANULARITY)

        linkable_dimensions.append(
            LinkableDimension(
                semantic_model_origin=semantic_model_origin,
                element_name=dimension.reference.element_name,
                dimension_type=DimensionType.TIME,
                entity_links=entity_links,
                join_path=tuple(join_path),
                time_granularity=time_granularity,
                date_part=None,
                properties=frozenset(properties),
            )
        )

        # Add the time dimension aggregated to a different date part.
        for date_part in DatePart:
            if time_granularity.to_int() <= date_part.to_int():
                linkable_dimensions.append(
                    LinkableDimension(
                        semantic_model_origin=semantic_model_origin,
                        element_name=dimension.reference.element_name,
                        dimension_type=DimensionType.TIME,
                        entity_links=entity_links,
                        join_path=tuple(join_path),
                        time_granularity=time_granularity,
                        date_part=date_part,
                        properties=frozenset(properties),
                    )
                )

    return linkable_dimensions


class ValidLinkableSpecResolver:
    """Figures out what linkable specs are valid for a given metric.

    e.g. Can you query the metric "bookings" by "listing__country_latest"?
    """

    def __init__(
        self,
        semantic_manifest: SemanticManifest,
        semantic_model_lookup: SemanticModelLookup,
        max_entity_links: int,
    ) -> None:
        """Constructor.

        Args:
            semantic_manifest: the model to use.
            semantic_model_lookup: used to look up entities for a semantic model.
            max_entity_links: the maximum number of joins to do when computing valid elements.
        """
        self._semantic_manifest = semantic_manifest
        self._semantic_model_lookup = semantic_model_lookup
        # Sort semantic models by name for consistency in building derived objects.
        self._semantic_models = sorted(self._semantic_manifest.semantic_models, key=lambda x: x.name)
        self._join_evaluator = SemanticModelJoinEvaluator(semantic_model_lookup)

        assert max_entity_links >= 0
        self._max_entity_links = max_entity_links

        # Map measures / entities to semantic models that contain them.
        self._entity_to_semantic_model: Dict[str, List[SemanticModel]] = defaultdict(list)
        self._measure_to_semantic_model: Dict[str, List[SemanticModel]] = defaultdict(list)

        for semantic_model in self._semantic_models:
            for entity in semantic_model.entities:
                self._entity_to_semantic_model[entity.reference.element_name].append(semantic_model)

        self._metric_to_linkable_element_sets: Dict[str, List[LinkableElementSet]] = {}
        self._joinable_metrics_for_semantic_models: Dict[SemanticModelReference, Set[MetricReference]] = {}

        start_time = time.time()
        for metric in self._semantic_manifest.metrics:
            linkable_sets_for_measure = []
            for measure in metric.measure_references:
                # Cumulative metrics currently can't be queried by other time granularities.
                if metric.type is MetricType.CUMULATIVE:
                    linkable_sets_for_measure.append(
                        self._get_linkable_element_set_for_measure(measure).filter(
                            with_any_of=LinkableElementProperty.all_properties(),
                            # Use filter() here becasue `without_all_of` param is only available on that method.
                            without_all_of=frozenset(
                                {
                                    LinkableElementProperty.METRIC_TIME,
                                    LinkableElementProperty.DERIVED_TIME_GRANULARITY,
                                }
                            ),
                        )
                    )
                elif (
                    metric.type is MetricType.SIMPLE
                    or metric.type is MetricType.DERIVED
                    or metric.type is MetricType.RATIO
                ):
                    linkable_sets_for_measure.append(self._get_linkable_element_set_for_measure(measure))
                elif metric.type is MetricType.CONVERSION:
                    conversion_type_params = metric.type_params.conversion_type_params
                    assert (
                        conversion_type_params
                    ), "A conversion metric should have type_params.conversion_type_params defined."
                    if measure == conversion_type_params.base_measure.measure_reference:
                        # Only can query against the base measure's linkable elements
                        # as it joins everything back to the base measure data set so
                        # there is no way of getting the conversion elements
                        linkable_sets_for_measure.append(self._get_linkable_element_set_for_measure(measure))
                else:
                    assert_values_exhausted(metric.type)

            self._metric_to_linkable_element_sets[metric.name] = linkable_sets_for_measure

        # This loop must happen after the one above so that _metric_to_linkable_element_sets is populated.
        for metric in self._semantic_manifest.metrics:
            metric_reference = MetricReference(metric.name)
            linkable_element_set_for_metric = self.get_linkable_elements_for_metrics([metric_reference])
            for linkable_entities in linkable_element_set_for_metric.path_key_to_linkable_entities.values():
                for linkable_entity in linkable_entities:
                    semantic_model_reference = linkable_entity.semantic_model_origin
                    metrics = self._joinable_metrics_for_semantic_models.get(semantic_model_reference, set())
                    metrics.add(metric_reference)
                    self._joinable_metrics_for_semantic_models[semantic_model_reference] = metrics

        # If no metrics are specified, the query interface supports querying distinct values for dimensions, entities,
        # and group by metrics.
        linkable_element_sets_for_no_metrics_queries: List[LinkableElementSet] = []
        for semantic_model in semantic_manifest.semantic_models:
            linkable_element_sets_for_no_metrics_queries.append(self._get_elements_in_semantic_model(semantic_model))
            joinable_metrics = self._joinable_metrics_for_semantic_models.get(semantic_model.reference, set())
            for entity in semantic_model.entities:
                linkable_metrics_set = LinkableElementSet(
                    path_key_to_linkable_metrics={
                        ElementPathKey(
                            element_name=metric.element_name,
                            element_type=LinkableElementType.METRIC,
                            entity_links=(entity.reference,),
                        ): (
                            LinkableMetric(
                                element_name=metric.element_name,
                                entity_links=(entity.reference,),
                                join_path=(
                                    SemanticModelJoinPathElement(
                                        semantic_model_reference=semantic_model.reference,
                                        join_on_entity=entity.reference,
                                    ),
                                ),
                                join_by_semantic_model=semantic_model.reference,
                                properties=frozenset({LinkableElementProperty.METRIC}),
                            ),
                        )
                        for metric in joinable_metrics
                    },
                )
                linkable_element_sets_for_no_metrics_queries.append(linkable_metrics_set)

        metric_time_elements_for_no_metrics = self._get_metric_time_elements(measure_reference=None)
        self._no_metric_linkable_element_set = LinkableElementSet.merge_by_path_key(
            linkable_element_sets_for_no_metrics_queries + [metric_time_elements_for_no_metrics]
        )

        logger.info(f"Building valid group-by-item indexes took: {time.time() - start_time:.2f}s")

    def _get_semantic_model_for_measure(self, measure_reference: MeasureReference) -> SemanticModel:
        semantic_models_where_measure_was_found = []
        for semantic_model in self._semantic_models:
            if any([x.reference.element_name == measure_reference.element_name for x in semantic_model.measures]):
                semantic_models_where_measure_was_found.append(semantic_model)

        if len(semantic_models_where_measure_was_found) == 0:
            raise ValueError(f"No semantic models were found with {measure_reference} in the model")
        elif len(semantic_models_where_measure_was_found) > 1:
            raise ValueError(
                f"Measure {measure_reference} was found in multiple semantic models:\n"
                f"{mf_pformat(semantic_models_where_measure_was_found)}"
            )
        return semantic_models_where_measure_was_found[0]

    def _get_joinable_metrics_for_semantic_model(self, semantic_model: SemanticModel) -> LinkableElementSet:
        linkable_metrics = []
        for metric_ref in self._joinable_metrics_for_semantic_models.get(semantic_model.reference, set()):
            for entity_link in [entity.reference for entity in semantic_model.entities]:
                linkable_metrics.append(
                    LinkableMetric(
                        element_name=metric_ref.element_name,
                        join_by_semantic_model=semantic_model.reference,
                        entity_links=(entity_link,),
                        properties=frozenset({LinkableElementProperty.METRIC}),
                        join_path=(),
                    )
                )
        return LinkableElementSet(
            path_key_to_linkable_metrics={
                linkable_metric.path_key: (linkable_metric,) for linkable_metric in linkable_metrics
            },
        )

    def _get_elements_in_semantic_model(self, semantic_model: SemanticModel) -> LinkableElementSet:
        """Gets the elements in the semantic model, without requiring any joins.

        Elements related to metric_time are handled separately in _get_metric_time_elements().
        """
        linkable_dimensions = []
        linkable_entities = []
        for entity in semantic_model.entities:
            linkable_entities.append(
                LinkableEntity(
                    semantic_model_origin=semantic_model.reference,
                    element_name=entity.reference.element_name,
                    entity_links=(),
                    join_path=(),
                    properties=frozenset({LinkableElementProperty.LOCAL, LinkableElementProperty.ENTITY}),
                )
            )
            for entity_link in self._semantic_model_lookup.entity_links_for_local_elements(semantic_model):
                # Avoid creating "booking_id__booking_id"
                if entity_link == entity.reference:
                    continue
                linkable_entities.append(
                    LinkableEntity(
                        semantic_model_origin=semantic_model.reference,
                        element_name=entity.reference.element_name,
                        entity_links=(entity_link,),
                        join_path=(),
                        properties=frozenset({LinkableElementProperty.LOCAL, LinkableElementProperty.ENTITY}),
                    )
                )

        for entity_link in self._semantic_model_lookup.entity_links_for_local_elements(semantic_model):
            dimension_properties = frozenset({LinkableElementProperty.LOCAL})
            for dimension in semantic_model.dimensions:
                dimension_type = dimension.type
                if dimension_type is DimensionType.CATEGORICAL:
                    linkable_dimensions.append(
                        LinkableDimension(
                            semantic_model_origin=semantic_model.reference,
                            element_name=dimension.reference.element_name,
                            dimension_type=DimensionType.CATEGORICAL,
                            entity_links=(entity_link,),
                            join_path=(),
                            properties=dimension_properties,
                            time_granularity=None,
                            date_part=None,
                        )
                    )
                elif dimension_type is DimensionType.TIME:
                    linkable_dimensions.extend(
                        _generate_linkable_time_dimensions(
                            semantic_model_origin=semantic_model.reference,
                            dimension=dimension,
                            entity_links=(entity_link,),
                            join_path=(),
                            with_properties=dimension_properties,
                        )
                    )
                else:
                    assert_values_exhausted(dimension_type)

        return LinkableElementSet(
            path_key_to_linkable_dimensions={
                linkable_dimension.path_key: (linkable_dimension,) for linkable_dimension in linkable_dimensions
            },
            path_key_to_linkable_entities={
                linkable_entity.path_key: (linkable_entity,) for linkable_entity in linkable_entities
            },
        )

    def _get_semantic_models_with_joinable_entity(
        self,
        left_semantic_model_reference: SemanticModelReference,
        entity_reference: EntityReference,
    ) -> Sequence[SemanticModel]:
        # May switch to non-cached implementation.
        semantic_models = self._entity_to_semantic_model[entity_reference.element_name]
        valid_semantic_models = []
        for semantic_model in semantic_models:
            if self._join_evaluator.is_valid_semantic_model_join(
                left_semantic_model_reference=left_semantic_model_reference,
                right_semantic_model_reference=semantic_model.reference,
                on_entity_reference=entity_reference,
            ):
                valid_semantic_models.append(semantic_model)
        return valid_semantic_models

    @staticmethod
    def _get_time_granularity_for_dimension(
        semantic_model: SemanticModel, time_dimension_reference: TimeDimensionReference
    ) -> TimeGranularity:
        matching_dimensions = tuple(
            dimension
            for dimension in semantic_model.dimensions
            if dimension.type is DimensionType.TIME and dimension.time_dimension_reference == time_dimension_reference
        )

        if len(matching_dimensions) != 1:
            raise RuntimeError(
                f"Did not find a matching time dimension for {time_dimension_reference} in {semantic_model}"
            )
        time_dimension = matching_dimensions[0]
        type_params = time_dimension.type_params
        assert (
            type_params is not None
        ), f"type_params should have been set for {time_dimension_reference} in {semantic_model}"

        assert (
            type_params.time_granularity is not None
        ), f"time_granularity should have been set for {time_dimension_reference} in {semantic_model}"

        return type_params.time_granularity

    def _get_metric_time_elements(self, measure_reference: Optional[MeasureReference] = None) -> LinkableElementSet:
        """Create elements for metric_time for a given measure in a semantic model.

        metric_time is a virtual dimension that is the same as aggregation time dimension for a measure, but with a
        different name. Because it doesn't actually exist in the semantic model, these elements need to be created based
        on what aggregation time dimension was used to define the measure.
        """
        measure_semantic_model: Optional[SemanticModel] = None
        if measure_reference:
            measure_semantic_model = self._get_semantic_model_for_measure(measure_reference)
            measure_agg_time_dimension_reference = measure_semantic_model.checked_agg_time_dimension_for_measure(
                measure_reference=measure_reference
            )
            defined_granularity = self._get_time_granularity_for_dimension(
                semantic_model=measure_semantic_model,
                time_dimension_reference=measure_agg_time_dimension_reference,
            )
        else:
            defined_granularity = DEFAULT_TIME_GRANULARITY

        # It's possible to aggregate measures to coarser time granularities (except with cumulative metrics).
        possible_metric_time_granularities = tuple(
            time_granularity
            for time_granularity in TimeGranularity
            if defined_granularity.is_smaller_than_or_equal(time_granularity)
        )

        # For each of the possible time granularities, create a LinkableDimension.
        path_key_to_linkable_dimensions: Dict[ElementPathKey, List[LinkableDimension]] = defaultdict(list)
        for time_granularity in possible_metric_time_granularities:
            possible_date_parts: Sequence[Optional[DatePart]] = (
                # No date part, just the metric time at a different grain.
                (None,)
                # date part of a metric time at a different grain.
                + tuple(date_part for date_part in DatePart if time_granularity.to_int() <= date_part.to_int())
            )

            for date_part in possible_date_parts:
                path_key = ElementPathKey(
                    element_name=MetricFlowReservedKeywords.METRIC_TIME.value,
                    element_type=LinkableElementType.TIME_DIMENSION,
                    entity_links=(),
                    time_granularity=time_granularity,
                    date_part=date_part,
                )
                path_key_to_linkable_dimensions[path_key].append(
                    LinkableDimension(
                        semantic_model_origin=measure_semantic_model.reference if measure_semantic_model else None,
                        element_name=MetricFlowReservedKeywords.METRIC_TIME.value,
                        dimension_type=DimensionType.TIME,
                        entity_links=(),
                        join_path=(),
                        # Anything that's not at the base time granularity of the measure's aggregation time dimension
                        # should be considered derived.
                        properties=(
                            frozenset({LinkableElementProperty.METRIC_TIME})
                            if time_granularity is defined_granularity and date_part is None
                            else frozenset(
                                {
                                    LinkableElementProperty.METRIC_TIME,
                                    LinkableElementProperty.DERIVED_TIME_GRANULARITY,
                                }
                            )
                        ),
                        time_granularity=time_granularity,
                        date_part=date_part,
                    )
                )

        return LinkableElementSet(
            path_key_to_linkable_dimensions={
                path_key: tuple(linkable_dimensions)
                for path_key, linkable_dimensions in path_key_to_linkable_dimensions.items()
            }
        )

    def _get_joined_elements(self, measure_semantic_model: SemanticModel) -> LinkableElementSet:
        """Get the elements that can be generated by joining other models to the given model."""
        # Create single-hop elements
        join_paths = []
        for entity in measure_semantic_model.entities:
            semantic_models = self._get_semantic_models_with_joinable_entity(
                left_semantic_model_reference=measure_semantic_model.reference,
                entity_reference=entity.reference,
            )
            for semantic_model in semantic_models:
                if semantic_model.name == measure_semantic_model.name:
                    continue
                join_paths.append(
                    SemanticModelJoinPath(
                        path_elements=(
                            SemanticModelJoinPathElement(
                                semantic_model_reference=semantic_model.reference,
                                join_on_entity=entity.reference,
                            ),
                        )
                    )
                )
        single_hop_elements = LinkableElementSet.merge_by_path_key(
            [
                self.create_linkable_element_set_from_join_path(
                    join_path=join_path,
                    with_properties=frozenset({LinkableElementProperty.JOINED}),
                )
                for join_path in join_paths
            ]
        )

        # Create multi-hop elements. At each iteration, we generate the list of valid elements based on the current join
        # path, extend all paths to include the next valid semantic model, then repeat.
        multi_hop_elements = LinkableElementSet()

        for _ in range(self._max_entity_links - 1):
            new_join_paths: List[SemanticModelJoinPath] = []
            for join_path in join_paths:
                new_join_paths.extend(
                    self._find_next_possible_paths(
                        measure_semantic_model=measure_semantic_model, current_join_path=join_path
                    )
                )

            if len(new_join_paths) == 0:
                break

            multi_hop_elements = LinkableElementSet.merge_by_path_key(
                (multi_hop_elements,)
                + tuple(
                    self.create_linkable_element_set_from_join_path(
                        join_path=new_join_path,
                        with_properties=frozenset({LinkableElementProperty.JOINED, LinkableElementProperty.MULTI_HOP}),
                    )
                    for new_join_path in new_join_paths
                )
            )
            join_paths = new_join_paths

        return LinkableElementSet.merge_by_path_key((single_hop_elements, multi_hop_elements))

    def _get_linkable_element_set_for_measure(
        self,
        measure_reference: MeasureReference,
        with_any_of: FrozenSet[LinkableElementProperty] = LinkableElementProperty.all_properties(),
        without_any_of: FrozenSet[LinkableElementProperty] = frozenset(),
    ) -> LinkableElementSet:
        """See get_linkable_element_set_for_measure()."""
        measure_semantic_model = self._get_semantic_model_for_measure(measure_reference)

        elements_in_semantic_model = self._get_elements_in_semantic_model(measure_semantic_model)
        metrics_linked_to_semantic_model = self._get_joinable_metrics_for_semantic_model(measure_semantic_model)
        metric_time_elements = self._get_metric_time_elements(measure_reference)
        joined_elements = self._get_joined_elements(measure_semantic_model)

        return LinkableElementSet.merge_by_path_key(
            (
                elements_in_semantic_model,
                metrics_linked_to_semantic_model,
                metric_time_elements,
                joined_elements,
            )
        ).filter(
            with_any_of=with_any_of,
            without_any_of=without_any_of,
        )

    def get_linkable_element_set_for_measure(
        self,
        measure_reference: MeasureReference,
        with_any_of: FrozenSet[LinkableElementProperty],
        without_any_of: FrozenSet[LinkableElementProperty],
    ) -> LinkableElementSet:
        """Get the valid linkable elements for the given measure."""
        return self._get_linkable_element_set_for_measure(
            measure_reference=measure_reference,
            with_any_of=with_any_of,
            without_any_of=without_any_of,
        )

    def get_linkable_elements_for_distinct_values_query(
        self,
        with_any_of: FrozenSet[LinkableElementProperty],
        without_any_of: FrozenSet[LinkableElementProperty],
    ) -> LinkableElementSet:
        """Returns queryable items for a distinct group-by-item values query.

        A distinct group-by-item values query does not include any metrics.
        """
        return self._no_metric_linkable_element_set.filter(with_any_of=with_any_of, without_any_of=without_any_of)

    def get_linkable_elements_for_metrics(
        self,
        metric_references: Sequence[MetricReference],
        with_any_of: FrozenSet[LinkableElementProperty] = LinkableElementProperty.all_properties(),
        without_any_of: FrozenSet[LinkableElementProperty] = frozenset(),
    ) -> LinkableElementSet:
        """Gets the valid linkable elements that are common to all requested metrics."""
        linkable_element_sets = []
        for metric_reference in metric_references:
            element_sets = self._metric_to_linkable_element_sets.get(metric_reference.element_name)
            if not element_sets:
                raise UnknownMetricLinkingError(f"Unknown metric: {metric_reference} in element set")

            # Using .only_unique_path_keys to exclude ambiguous elements where there are multiple join paths to get
            # a dimension / entity.
            metric_result = LinkableElementSet.intersection_by_path_key(
                [
                    element_set.only_unique_path_keys.filter(with_any_of=with_any_of, without_any_of=without_any_of)
                    for element_set in element_sets
                ]
            )
            linkable_element_sets.append(metric_result)

        intersection_set = LinkableElementSet.intersection_by_path_key(linkable_element_sets)
        return intersection_set

    def _find_next_possible_paths(
        self, measure_semantic_model: SemanticModel, current_join_path: SemanticModelJoinPath
    ) -> Sequence[SemanticModelJoinPath]:
        """Generate the set of possible paths that are 1 semantic model join longer that the "current_join_path"."""
        last_semantic_model_in_path = self._semantic_model_lookup.get_by_reference(
            current_join_path.last_semantic_model_reference
        )
        assert last_semantic_model_in_path
        new_join_paths = []

        for entity in last_semantic_model_in_path.entities:
            entity_reference = entity.reference

            # Don't create cycles in the join path by joining on the same entity.
            if entity_reference in set(x.join_on_entity for x in current_join_path.path_elements):
                continue

            semantic_models_that_can_be_joined = self._get_semantic_models_with_joinable_entity(
                left_semantic_model_reference=last_semantic_model_in_path.reference,
                entity_reference=entity.reference,
            )
            for semantic_model in semantic_models_that_can_be_joined:
                # Don't create cycles in the join path by repeating a semantic model in the path.
                if semantic_model.name == measure_semantic_model.name or any(
                    tuple(
                        path_element.semantic_model_reference == semantic_model.reference
                        for path_element in current_join_path.path_elements
                    )
                ):
                    continue

                new_join_path = SemanticModelJoinPath(
                    path_elements=current_join_path.path_elements
                    + (
                        SemanticModelJoinPathElement(
                            semantic_model_reference=semantic_model.reference, join_on_entity=entity_reference
                        ),
                    )
                )
                new_join_paths.append(new_join_path)

        return new_join_paths

    def create_linkable_element_set_from_join_path(
        self,
        join_path: SemanticModelJoinPath,
        with_properties: FrozenSet[LinkableElementProperty],
    ) -> LinkableElementSet:
        """Given the current path, generate the respective linkable elements from the last semantic model in the path."""
        entity_links = tuple(x.join_on_entity for x in join_path.path_elements)

        semantic_model = self._semantic_model_lookup.get_by_reference(join_path.last_semantic_model_reference)
        assert semantic_model

        linkable_dimensions: List[LinkableDimension] = []
        linkable_entities: List[LinkableEntity] = []
        linkable_metrics: List[LinkableMetric] = []

        for dimension in semantic_model.dimensions:
            dimension_type = dimension.type
            if dimension_type == DimensionType.CATEGORICAL:
                linkable_dimensions.append(
                    LinkableDimension(
                        semantic_model_origin=semantic_model.reference,
                        element_name=dimension.reference.element_name,
                        dimension_type=DimensionType.CATEGORICAL,
                        entity_links=entity_links,
                        join_path=join_path.path_elements,
                        properties=with_properties,
                        time_granularity=None,
                        date_part=None,
                    )
                )
            elif dimension_type == DimensionType.TIME:
                linkable_dimensions.extend(
                    _generate_linkable_time_dimensions(
                        semantic_model_origin=semantic_model.reference,
                        dimension=dimension,
                        entity_links=entity_links,
                        join_path=join_path.path_elements,
                        with_properties=with_properties,
                    )
                )
            else:
                raise RuntimeError(f"Unhandled type: {dimension_type}")

        for entity in semantic_model.entities:
            # Avoid creating "booking_id__booking_id"
            if entity.reference != entity_links[-1]:
                linkable_entities.append(
                    LinkableEntity(
                        semantic_model_origin=semantic_model.reference,
                        element_name=entity.reference.element_name,
                        entity_links=entity_links,
                        join_path=join_path.path_elements,
                        properties=with_properties.union({LinkableElementProperty.ENTITY}),
                    )
                )

        linkable_metrics = [
            LinkableMetric(
                element_name=metric.element_name,
                entity_links=entity_links,
                join_path=join_path.path_elements,
                join_by_semantic_model=semantic_model.reference,
                properties=with_properties.union({LinkableElementProperty.METRIC}),
            )
            for metric in self._joinable_metrics_for_semantic_models.get(join_path.last_semantic_model_reference, set())
        ]

        return LinkableElementSet(
            path_key_to_linkable_dimensions={
                linkable_dimension.path_key: (linkable_dimension,) for linkable_dimension in linkable_dimensions
            },
            path_key_to_linkable_entities={
                linkable_entity.path_key: (linkable_entity,) for linkable_entity in linkable_entities
            },
            path_key_to_linkable_metrics={
                linkable_metric.path_key: (linkable_metric,) for linkable_metric in linkable_metrics
            },
        )
