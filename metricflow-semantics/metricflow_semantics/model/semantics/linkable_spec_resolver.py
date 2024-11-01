from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, FrozenSet, List, Optional, Sequence, Set, Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols.dimension import Dimension, DimensionType
from dbt_semantic_interfaces.protocols.metric import Metric
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.protocols.semantic_model import SemanticModel
from dbt_semantic_interfaces.references import (
    EntityReference,
    MeasureReference,
    MetricReference,
    SemanticModelReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums import MetricType
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from dbt_semantic_interfaces.validations.unique_valid_name import MetricFlowReservedKeywords

from metricflow_semantics.errors.error_classes import UnknownMetricLinkingError
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.pretty_print import mf_pformat
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantic_model_derivation import SemanticModelDerivation
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_element import (
    ElementPathKey,
    LinkableDimension,
    LinkableEntity,
    LinkableMetric,
    MetricSubqueryJoinPathElement,
    SemanticModelJoinPath,
    SemanticModelJoinPathElement,
    SemanticModelToMetricSubqueryJoinPath,
)
from metricflow_semantics.model.semantics.linkable_element_set import LinkableElementSet
from metricflow_semantics.model.semantics.semantic_model_helper import SemanticModelHelper
from metricflow_semantics.model.semantics.semantic_model_join_evaluator import SemanticModelJoinEvaluator
from metricflow_semantics.specs.time_dimension_spec import DEFAULT_TIME_GRANULARITY
from metricflow_semantics.time.granularity import ExpandedTimeGranularity
from metricflow_semantics.time.time_spine_source import TimeSpineSource

if TYPE_CHECKING:
    from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup


logger = logging.getLogger(__name__)


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
        self._time_spine_sources = TimeSpineSource.build_standard_time_spine_sources(self._semantic_manifest)
        self._custom_granularities = TimeSpineSource.build_custom_granularities(list(self._time_spine_sources.values()))

        assert max_entity_links >= 0
        self._max_entity_links = max_entity_links

        # Map measures / entities to semantic models that contain them.
        self._entity_to_semantic_model: Dict[str, List[SemanticModel]] = defaultdict(list)
        self._semantic_model_reference_to_semantic_model: Dict[SemanticModelReference, SemanticModel] = {}
        for semantic_model in self._semantic_models:
            for entity in semantic_model.entities:
                self._entity_to_semantic_model[entity.reference.element_name].append(semantic_model)
            self._semantic_model_reference_to_semantic_model[semantic_model.reference] = semantic_model

        self._metric_to_linkable_element_sets: Dict[str, List[LinkableElementSet]] = {}
        self._metric_references_to_metrics: Dict[MetricReference, Metric] = {}
        self._joinable_metrics_for_entities: Dict[EntityReference, Set[MetricSubqueryJoinPathElement]] = defaultdict(
            set
        )

        # Cache for `_get_joined_elements()`.
        self._semantic_model_reference_to_joined_elements: Dict[SemanticModelReference, LinkableElementSet] = {}

        start_time = time.time()
        for metric in self._semantic_manifest.metrics:
            self._metric_references_to_metrics[MetricReference(metric.name)] = metric
            linkable_sets_for_measure = []
            for measure in metric.measure_references:
                if metric.type is MetricType.CUMULATIVE:
                    linkable_sets_for_measure.append(
                        self._get_linkable_element_set_for_measure(
                            measure,
                            LinkableElementFilter(without_any_of=frozenset({LinkableElementProperty.DATE_PART})),
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

        # Populate storage dicts with linkable metrics. This loop must happen after the one above so that
        # _metric_to_linkable_element_sets is populated with entities and dimensions.
        linkable_metrics_start_time = time.time()
        for metric in self._semantic_manifest.metrics:
            # Cumulative metrics and time offset metrics require grouping by metric_time, which is not yet available for
            # linkable metrics. So skip those.
            if self._metric_requires_metric_time(metric):
                continue
            metric_reference = MetricReference(metric.name)
            linkable_element_set_for_metric = self.get_linkable_elements_for_metrics([metric_reference])

            defined_from_semantic_models = tuple(
                self._semantic_model_lookup.measure_lookup.get_properties(
                    input_measure.measure_reference
                ).model_reference
                for input_measure in metric.input_measures
            )

            for linkable_entities in linkable_element_set_for_metric.path_key_to_linkable_entities.values():
                for linkable_entity in linkable_entities:
                    # TODO: some users encounter a situation in which the entity reference is in the entity links. Debug why.
                    if linkable_entity.reference in linkable_entity.entity_links:
                        logger.debug(
                            LazyFormat(
                                lambda: f"Found entity reference in entity links for linkable entity: {linkable_entity}"
                            )
                        )
                        continue
                    metric_subquery_join_path_element = MetricSubqueryJoinPathElement(
                        metric_reference=metric_reference,
                        derived_from_semantic_models=defined_from_semantic_models,
                        join_on_entity=linkable_entity.reference,
                        entity_links=linkable_entity.entity_links,
                        metric_to_entity_join_path=(linkable_entity.join_path if linkable_entity.join_path else None),
                    )
                    self._joinable_metrics_for_entities[linkable_entity.reference].add(
                        metric_subquery_join_path_element
                    )
                    # TODO: update _metric_to_linkable_element_sets to have linkable metrics

        # If no metrics are specified, the query interface supports querying distinct values for dimensions, entities,
        # and group by metrics.
        linkable_element_sets_for_no_metrics_queries: List[LinkableElementSet] = []
        # Temp: separate loop to track latency for linkable metrics. TODO: combine with loop below.
        for semantic_model in semantic_manifest.semantic_models:
            linkable_element_sets_for_no_metrics_queries.append(
                self.get_joinable_metrics_for_semantic_model(
                    semantic_model, SemanticModelJoinPath(left_semantic_model_reference=semantic_model.reference)
                )
            )
        logger.debug(
            LazyFormat(
                lambda: f"Building valid linkable metrics took: {time.time() - linkable_metrics_start_time:.2f}s"
            )
        )

        for semantic_model in semantic_manifest.semantic_models:
            linkable_element_sets_for_no_metrics_queries.append(self._get_elements_in_semantic_model(semantic_model))

        metric_time_elements_for_no_metrics = self._get_metric_time_elements(measure_reference=None)
        self._no_metric_linkable_element_set = LinkableElementSet.merge_by_path_key(
            linkable_element_sets_for_no_metrics_queries + [metric_time_elements_for_no_metrics]
        )

        logger.debug(LazyFormat(lambda: f"Building valid group-by-item indexes took: {time.time() - start_time:.2f}s"))

    def _is_semantic_model_scd(self, semantic_model: SemanticModel) -> bool:
        """Whether the semantic model's underlying table is an SCD."""
        return any(dim.validity_params is not None for dim in semantic_model.dimensions)

    def _generate_linkable_time_dimensions(
        self,
        semantic_model_origin: SemanticModelReference,
        dimension: Dimension,
        entity_links: Tuple[EntityReference, ...],
        join_path: SemanticModelJoinPath,
        with_properties: FrozenSet[LinkableElementProperty],
    ) -> Sequence[LinkableDimension]:
        """Generates different versions of the given time dimension with all compatible time granularities & date parts."""
        defined_time_granularity = (
            dimension.type_params.time_granularity if dimension.type_params else DEFAULT_TIME_GRANULARITY
        )

        def create(
            time_granularity: ExpandedTimeGranularity, date_part: Optional[DatePart] = None
        ) -> LinkableDimension:
            properties = set(with_properties)
            if time_granularity.is_custom_granularity or time_granularity.base_granularity != defined_time_granularity:
                properties.add(LinkableElementProperty.DERIVED_TIME_GRANULARITY)
            if date_part:
                properties.add(LinkableElementProperty.DATE_PART)
            return LinkableDimension.create(
                defined_in_semantic_model=semantic_model_origin,
                element_name=dimension.reference.element_name,
                dimension_type=DimensionType.TIME,
                entity_links=entity_links,
                join_path=join_path,
                time_granularity=time_granularity,
                date_part=date_part,
                properties=tuple(sorted(properties)),
            )

        # Build linkable dimensions for all compatible standard granularities and date parts.
        linkable_dimensions = []
        for time_granularity in TimeGranularity:
            if time_granularity.to_int() < defined_time_granularity.to_int():
                continue
            expanded_granularity = ExpandedTimeGranularity.from_time_granularity(time_granularity)
            linkable_dimensions.append(create(expanded_granularity))
            for date_part in DatePart:
                if time_granularity.to_int() <= date_part.to_int():
                    linkable_dimensions.append(create(time_granularity=expanded_granularity, date_part=date_part))

        # Build linkable dimensions for all compatible custom granularities.
        for custom_grain in self._custom_granularities.values():
            if custom_grain.base_granularity.to_int() >= defined_time_granularity.to_int():
                linkable_dimensions.append(create(custom_grain))

        return linkable_dimensions

    def _metric_requires_metric_time(self, metric: Metric) -> bool:
        """Checks if the metric can only be queried with metric_time. Also checks input metrics.

        True if the metric uses cumulative time component or a time offset.
        """
        metrics_to_check = [metric]
        while metrics_to_check:
            metric_to_check = metrics_to_check.pop()
            if metric_to_check.type_params.cumulative_type_params and (
                metric_to_check.type_params.cumulative_type_params.window is not None
                or metric_to_check.type_params.cumulative_type_params.grain_to_date is not None
            ):
                return True
            for input_metric in metric_to_check.input_metrics:
                if input_metric.offset_window is not None or input_metric.offset_to_grain is not None:
                    return True
                metric_for_input_metric = self._metric_references_to_metrics.get(MetricReference(input_metric.name))
                assert (
                    metric_for_input_metric
                ), f"Did not find input metric {input_metric.name} in registered metrics. This indicates internal misconfiguration."
                metrics_to_check.append(metric_for_input_metric)

        return False

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

    def get_joinable_metrics_for_semantic_model(
        self, semantic_model: SemanticModel, using_join_path: SemanticModelJoinPath
    ) -> LinkableElementSet:
        """Get the set of linkable metrics that can be joined to this semantic model.

        From @courtneyholcomb re: `using_join_path`:
            That would be the join path to get from the outer query to the metric subquery. If you query metric1
            filtered by metric2 (grouped by entity1), it would be the join path to get from metric1 to entity1. If
            entity1 is local to the semantic model where metric1's input measures are defined, no join path is
            necessary.
        """
        properties = frozenset({LinkableElementProperty.METRIC, LinkableElementProperty.JOINED})
        if self._is_semantic_model_scd(semantic_model):
            properties = properties.union({LinkableElementProperty.SCD_HOP})

        join_path_has_path_links = len(using_join_path.path_elements) > 0
        if join_path_has_path_links:
            assert semantic_model.reference == using_join_path.last_semantic_model_reference, (
                "Last join path element should match semantic model when building LinkableMetrics. "
                f"Got semantic model: {semantic_model.reference.semantic_model_name}; "
                f"last join path element: {using_join_path.last_semantic_model_reference.semantic_model_name}",
            )
            # Temp: disable LinkableMetrics with outer join path until there is an interface to specify it.
            # The result's properties = properties.union(frozenset({LinkableElementProperty.MULTI_HOP}))
            return LinkableElementSet()

        path_key_to_linkable_metrics: Dict[ElementPathKey, Tuple[LinkableMetric, ...]] = {}
        for entity_reference in [entity.reference for entity in semantic_model.entities]:
            # Avoid creating an entity link cycle.
            if join_path_has_path_links and entity_reference in using_join_path.entity_links:
                continue
            for metric_subquery_join_path_element in self._joinable_metrics_for_entities[entity_reference]:
                linkable_metric = LinkableMetric.create(
                    properties=properties,
                    join_path=SemanticModelToMetricSubqueryJoinPath(
                        metric_subquery_join_path_element=metric_subquery_join_path_element,
                        semantic_model_join_path=using_join_path,
                    ),
                )
                path_key_to_linkable_metrics[linkable_metric.path_key] = path_key_to_linkable_metrics.get(
                    linkable_metric.path_key, ()
                ) + (linkable_metric,)

        return LinkableElementSet(path_key_to_linkable_metrics=path_key_to_linkable_metrics)

    def _get_elements_in_semantic_model(self, semantic_model: SemanticModel) -> LinkableElementSet:
        """Gets the elements in the semantic model, without requiring any joins.

        Elements related to metric_time are handled separately in _get_metric_time_elements().
        Linkable metrics are not considered local to the semantic model since they always require a join.
        """
        semantic_model_is_scd = self._is_semantic_model_scd(semantic_model)

        linkable_dimensions = []
        linkable_entities = []

        entity_properties = frozenset({LinkableElementProperty.LOCAL, LinkableElementProperty.ENTITY})
        if semantic_model_is_scd:
            entity_properties = entity_properties.union({LinkableElementProperty.SCD_HOP})

        for entity in semantic_model.entities:
            linkable_entities.append(
                LinkableEntity.create(
                    defined_in_semantic_model=semantic_model.reference,
                    element_name=entity.reference.element_name,
                    entity_links=(),
                    join_path=SemanticModelJoinPath(
                        left_semantic_model_reference=semantic_model.reference,
                    ),
                    properties=entity_properties,
                )
            )
            for entity_link in SemanticModelHelper.entity_links_for_local_elements(semantic_model):
                # Avoid creating "booking_id__booking_id"
                if entity_link == entity.reference:
                    continue
                linkable_entities.append(
                    LinkableEntity.create(
                        defined_in_semantic_model=semantic_model.reference,
                        element_name=entity.reference.element_name,
                        entity_links=(entity_link,),
                        join_path=SemanticModelJoinPath(
                            left_semantic_model_reference=semantic_model.reference,
                        ),
                        properties=entity_properties,
                    )
                )

        dimension_properties = frozenset({LinkableElementProperty.LOCAL})
        if semantic_model_is_scd:
            dimension_properties = dimension_properties.union({LinkableElementProperty.SCD_HOP})

        for entity_link in SemanticModelHelper.entity_links_for_local_elements(semantic_model):
            for dimension in semantic_model.dimensions:
                dimension_type = dimension.type
                if dimension_type is DimensionType.CATEGORICAL:
                    linkable_dimensions.append(
                        LinkableDimension.create(
                            defined_in_semantic_model=semantic_model.reference,
                            element_name=dimension.reference.element_name,
                            dimension_type=DimensionType.CATEGORICAL,
                            entity_links=(entity_link,),
                            join_path=SemanticModelJoinPath(
                                left_semantic_model_reference=semantic_model.reference,
                            ),
                            properties=dimension_properties,
                            time_granularity=None,
                            date_part=None,
                        )
                    )
                elif dimension_type is DimensionType.TIME:
                    linkable_dimensions.extend(
                        self._generate_linkable_time_dimensions(
                            semantic_model_origin=semantic_model.reference,
                            dimension=dimension,
                            entity_links=(entity_link,),
                            join_path=SemanticModelJoinPath(
                                left_semantic_model_reference=semantic_model.reference,
                            ),
                            with_properties=dimension_properties,
                        )
                    )
                else:
                    assert_values_exhausted(dimension_type)

        path_key_to_linkable_dimensions: Dict[ElementPathKey, Tuple[LinkableDimension, ...]] = {}
        for linkable_dimension in linkable_dimensions:
            path_key_to_linkable_dimensions[linkable_dimension.path_key] = path_key_to_linkable_dimensions.get(
                linkable_dimension.path_key, ()
            ) + (linkable_dimension,)
        path_key_to_linkable_entities: Dict[ElementPathKey, Tuple[LinkableEntity, ...]] = {}
        for linkable_entity in linkable_entities:
            path_key_to_linkable_entities[linkable_entity.path_key] = path_key_to_linkable_entities.get(
                linkable_entity.path_key, ()
            ) + (linkable_entity,)
        return LinkableElementSet(
            path_key_to_linkable_dimensions=path_key_to_linkable_dimensions,
            path_key_to_linkable_entities=path_key_to_linkable_entities,
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
        defined_granularity: Optional[ExpandedTimeGranularity] = None
        if measure_reference:
            measure_semantic_model = self._get_semantic_model_for_measure(measure_reference)
            semantic_model_is_scd = self._is_semantic_model_scd(measure_semantic_model)
            measure_agg_time_dimension_reference = measure_semantic_model.checked_agg_time_dimension_for_measure(
                measure_reference=measure_reference
            )
            min_granularity = self._get_time_granularity_for_dimension(
                semantic_model=measure_semantic_model,
                time_dimension_reference=measure_agg_time_dimension_reference,
            )
            defined_granularity = ExpandedTimeGranularity.from_time_granularity(min_granularity)
        else:
            # If querying metric_time without metrics, will query from time spines.
            # Defaults to DAY granularity if available in time spines, else smallest available granularity.
            min_granularity = min(self._time_spine_sources.keys())
            semantic_model_is_scd = False
        possible_metric_time_granularities = tuple(
            ExpandedTimeGranularity.from_time_granularity(time_granularity)
            for time_granularity in TimeGranularity
            if min_granularity.is_smaller_than_or_equal(time_granularity)
        ) + tuple(
            [
                custom_granularity
                for custom_granularity in self._custom_granularities.values()
                if min_granularity.is_smaller_than_or_equal(custom_granularity.base_granularity)
            ]
        )

        # For each granularity, will create one LinkableDimension with no date part, and one for each compatible date part.
        # TODO: group by resolution has different logic than source node builder for combining date part w/ grain. Fix.
        path_key_to_linkable_dimensions: Dict[ElementPathKey, List[LinkableDimension]] = defaultdict(list)
        for time_granularity in possible_metric_time_granularities:
            possible_date_parts: Tuple[Optional[DatePart], ...] = (None,)
            if not time_granularity.is_custom_granularity:
                possible_date_parts += tuple(
                    date_part
                    for date_part in DatePart
                    if time_granularity.base_granularity.to_int() <= date_part.to_int()
                )

            for date_part in possible_date_parts:
                properties = {LinkableElementProperty.METRIC_TIME}
                if time_granularity != defined_granularity:
                    properties.add(LinkableElementProperty.DERIVED_TIME_GRANULARITY)
                if date_part:
                    properties.add(LinkableElementProperty.DATE_PART)
                if semantic_model_is_scd:
                    properties.add(LinkableElementProperty.SCD_HOP)
                linkable_dimension = LinkableDimension.create(
                    defined_in_semantic_model=measure_semantic_model.reference if measure_semantic_model else None,
                    element_name=MetricFlowReservedKeywords.METRIC_TIME.value,
                    dimension_type=DimensionType.TIME,
                    entity_links=(),
                    join_path=SemanticModelJoinPath(
                        left_semantic_model_reference=(
                            measure_semantic_model.reference
                            if measure_semantic_model
                            else SemanticModelDerivation.VIRTUAL_SEMANTIC_MODEL_REFERENCE
                        ),
                    ),
                    properties=frozenset(properties),
                    time_granularity=time_granularity,
                    date_part=date_part,
                )
                path_key_to_linkable_dimensions[linkable_dimension.path_key].append(linkable_dimension)

        return LinkableElementSet(
            path_key_to_linkable_dimensions={
                path_key: tuple(linkable_dimensions)
                for path_key, linkable_dimensions in path_key_to_linkable_dimensions.items()
            }
        )

    def _get_joined_elements(self, measure_semantic_model_reference: SemanticModelReference) -> LinkableElementSet:
        """Get the elements that can be generated by joining other models to the given model."""
        result = self._semantic_model_reference_to_joined_elements.get(measure_semantic_model_reference)
        if result is not None:
            return result

        result = self._get_joined_elements_without_cache(measure_semantic_model_reference)
        self._semantic_model_reference_to_joined_elements[measure_semantic_model_reference] = result
        return result

    def _get_joined_elements_without_cache(
        self, measure_semantic_model_reference: SemanticModelReference
    ) -> LinkableElementSet:
        measure_semantic_model = self._semantic_model_reference_to_semantic_model[measure_semantic_model_reference]
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
                    SemanticModelJoinPath.from_single_element(
                        left_semantic_model_reference=measure_semantic_model.reference,
                        right_semantic_model_reference=semantic_model.reference,
                        join_on_entity=entity.reference,
                    )
                )
        single_hop_elements = LinkableElementSet.merge_by_path_key(
            [self.create_linkable_element_set_from_join_path(join_path) for join_path in join_paths]
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
                    self.create_linkable_element_set_from_join_path(new_join_path) for new_join_path in new_join_paths
                )
            )
            join_paths = new_join_paths

        return LinkableElementSet.merge_by_path_key((single_hop_elements, multi_hop_elements))

    def _get_linkable_element_set_for_measure(
        self,
        measure_reference: MeasureReference,
        element_filter: LinkableElementFilter = LinkableElementFilter(),
    ) -> LinkableElementSet:
        """See get_linkable_element_set_for_measure()."""
        measure_semantic_model = self._get_semantic_model_for_measure(measure_reference)

        elements_in_semantic_model = self._get_elements_in_semantic_model(measure_semantic_model)

        # Filter out group-by metrics if not specified by the property as there can be a large number of them.
        if LinkableElementProperty.METRIC not in element_filter.without_any_of:
            metrics_linked_to_semantic_model = self.get_joinable_metrics_for_semantic_model(
                semantic_model=measure_semantic_model,
                using_join_path=SemanticModelJoinPath(left_semantic_model_reference=measure_semantic_model.reference),
            )
        else:
            metrics_linked_to_semantic_model = LinkableElementSet()

        metric_time_elements = self._get_metric_time_elements(measure_reference)
        joined_elements = self._get_joined_elements(measure_semantic_model.reference)

        return LinkableElementSet.merge_by_path_key(
            (
                elements_in_semantic_model,
                metrics_linked_to_semantic_model,
                metric_time_elements,
                joined_elements,
            )
        ).filter(element_filter)

    def get_linkable_element_set_for_measure(
        self,
        measure_reference: MeasureReference,
        element_filter: LinkableElementFilter,
    ) -> LinkableElementSet:
        """Get the valid linkable elements for the given measure."""
        return self._get_linkable_element_set_for_measure(
            measure_reference=measure_reference,
            element_filter=element_filter,
        )

    def get_linkable_elements_for_distinct_values_query(
        self,
        element_filter: LinkableElementFilter,
    ) -> LinkableElementSet:
        """Returns queryable items for a distinct group-by-item values query.

        A distinct group-by-item values query does not include any metrics.
        """
        return self._no_metric_linkable_element_set.filter(element_filter)

    # TODO: the results of this method don't actually match what will be allowed for the metric. This method checks
    # _metric_to_linkable_element_sets, while the actual group by resolution DAG calls _get_linkable_element_set_for_measure.
    def get_linkable_elements_for_metrics(
        self,
        metric_references: Sequence[MetricReference],
        element_filter: LinkableElementFilter = LinkableElementFilter(),
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
                [element_set.only_unique_path_keys.filter(element_filter) for element_set in element_sets]
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
                    left_semantic_model_reference=current_join_path.left_semantic_model_reference,
                    path_elements=current_join_path.path_elements
                    + (
                        SemanticModelJoinPathElement(
                            semantic_model_reference=semantic_model.reference, join_on_entity=entity_reference
                        ),
                    ),
                )
                new_join_paths.append(new_join_path)

        return new_join_paths

    def create_linkable_element_set_from_join_path(
        self,
        join_path: SemanticModelJoinPath,
    ) -> LinkableElementSet:
        """Given the current path, generate the respective linkable elements from the last semantic model in the path."""
        semantic_model = self._semantic_model_lookup.get_by_reference(join_path.last_semantic_model_reference)
        assert (
            semantic_model
        ), f"Semantic model {join_path.last_semantic_model_reference.semantic_model_name} is in join path but does not exist in SemanticModelLookup"

        properties = frozenset({LinkableElementProperty.JOINED})
        if len(join_path.path_elements) > 1:
            properties = properties.union({LinkableElementProperty.MULTI_HOP})

        # If any of the semantic models in the join path is an SCD, add SCD_HOP
        for reference_to_derived_model in join_path.derived_from_semantic_models:
            derived_model = self._semantic_model_lookup.get_by_reference(reference_to_derived_model)
            assert (
                derived_model
            ), f"Semantic model {reference_to_derived_model.semantic_model_name} is in join path but does not exist in SemanticModelLookup"

            if self._is_semantic_model_scd(derived_model):
                properties = properties.union({LinkableElementProperty.SCD_HOP})
                break

        linkable_dimensions: List[LinkableDimension] = []
        linkable_entities: List[LinkableEntity] = []

        for dimension in semantic_model.dimensions:
            dimension_type = dimension.type
            if dimension_type == DimensionType.CATEGORICAL:
                linkable_dimensions.append(
                    LinkableDimension.create(
                        defined_in_semantic_model=semantic_model.reference,
                        element_name=dimension.reference.element_name,
                        dimension_type=DimensionType.CATEGORICAL,
                        entity_links=join_path.entity_links,
                        join_path=join_path,
                        properties=properties,
                        time_granularity=None,
                        date_part=None,
                    )
                )
            elif dimension_type == DimensionType.TIME:
                linkable_dimensions.extend(
                    self._generate_linkable_time_dimensions(
                        semantic_model_origin=semantic_model.reference,
                        dimension=dimension,
                        entity_links=join_path.entity_links,
                        join_path=join_path,
                        with_properties=properties,
                    )
                )
            else:
                raise RuntimeError(f"Unhandled type: {dimension_type}")

        for entity in semantic_model.entities:
            # Avoid creating "booking_id__booking_id"
            if entity.reference != join_path.last_entity_link:
                linkable_entities.append(
                    LinkableEntity.create(
                        defined_in_semantic_model=semantic_model.reference,
                        element_name=entity.reference.element_name,
                        entity_links=join_path.entity_links,
                        join_path=join_path,
                        properties=properties.union({LinkableElementProperty.ENTITY}),
                    )
                )

        path_key_to_linkable_dimensions: Dict[ElementPathKey, Tuple[LinkableDimension, ...]] = {}
        for linkable_dimension in linkable_dimensions:
            path_key_to_linkable_dimensions[linkable_dimension.path_key] = path_key_to_linkable_dimensions.get(
                linkable_dimension.path_key, ()
            ) + (linkable_dimension,)
        path_key_to_linkable_entities: Dict[ElementPathKey, Tuple[LinkableEntity, ...]] = {}
        for linkable_entity in linkable_entities:
            path_key_to_linkable_entities[linkable_entity.path_key] = path_key_to_linkable_entities.get(
                linkable_entity.path_key, ()
            ) + (linkable_entity,)
        return LinkableElementSet(
            path_key_to_linkable_dimensions=path_key_to_linkable_dimensions,
            path_key_to_linkable_entities=path_key_to_linkable_entities,
            path_key_to_linkable_metrics=self.get_joinable_metrics_for_semantic_model(
                semantic_model=semantic_model, using_join_path=join_path
            ).path_key_to_linkable_metrics,
        )
