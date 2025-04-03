from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, FrozenSet, List, Optional, Sequence, Tuple

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
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow_semantics.errors.error_classes import UnknownMetricLinkingError
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_element import (
    ElementPathKey,
    LinkableDimension,
    LinkableEntity,
    LinkableMetric,
    SemanticModelJoinPath,
    SemanticModelToMetricSubqueryJoinPath,
)
from metricflow_semantics.model.semantics.linkable_element_set import LinkableElementSet
from metricflow_semantics.model.semantics.linkable_spec_index import LinkableSpecIndex
from metricflow_semantics.model.semantics.manifest_object_lookup import SemanticManifestObjectLookup
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

    def __init__(  # noqa: D107
        self,
        semantic_manifest: SemanticManifest,
        semantic_model_lookup: SemanticModelLookup,
        manifest_object_lookup: SemanticManifestObjectLookup,
        linkable_spec_index: LinkableSpecIndex,
    ) -> None:
        self._semantic_manifest = semantic_manifest
        self._semantic_model_lookup = semantic_model_lookup
        self._time_spine_sources = TimeSpineSource.build_standard_time_spine_sources(self._semantic_manifest)
        self._custom_granularities = TimeSpineSource.build_custom_granularities(list(self._time_spine_sources.values()))
        self._linkable_spec_index = linkable_spec_index
        self._manifest_object_lookup = manifest_object_lookup

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
            time_granularity: Optional[ExpandedTimeGranularity] = None, date_part: Optional[DatePart] = None
        ) -> LinkableDimension:
            properties = set(with_properties)
            if time_granularity and (
                time_granularity.is_custom_granularity or time_granularity.base_granularity != defined_time_granularity
            ):
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
            if defined_time_granularity.to_int() <= date_part.to_int():
                linkable_dimensions.append(create(date_part=date_part))

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
                metric_for_input_metric = self._manifest_object_lookup.get_metric_by_reference(
                    MetricReference(input_metric.name)
                )
                assert (
                    metric_for_input_metric
                ), f"Did not find input metric {input_metric.name} in registered metrics. This indicates internal misconfiguration."
                metrics_to_check.append(metric_for_input_metric)

        return False

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
            for metric_subquery_join_path_element in self._linkable_spec_index.joinable_metrics_for_entities[
                entity_reference
            ]:
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

    def _get_joined_elements(self, measure_semantic_model_reference: SemanticModelReference) -> LinkableElementSet:
        """Get the elements that can be generated by joining other models to the given model."""
        result = self._linkable_spec_index.semantic_model_reference_to_joined_elements.get(
            measure_semantic_model_reference
        )
        if result is None:
            raise RuntimeError(
                LazyFormat(
                    "Did not find the given semantic model in the index",
                    measure_semantic_model_reference=measure_semantic_model_reference,
                ).evaluated_value
            )
        return result

    def _get_linkable_element_set_for_measure(
        self,
        measure_reference: MeasureReference,
        element_filter: LinkableElementFilter = LinkableElementFilter(),
    ) -> LinkableElementSet:
        """See get_linkable_element_set_for_measure()."""
        measure_semantic_model = self._manifest_object_lookup.get_semantic_model_containing_measure(measure_reference)
        elements_in_semantic_model = self._linkable_spec_index.semantic_model_reference_to_local_elements[
            measure_semantic_model.reference
        ]

        # Filter out group-by metrics if not specified by the property as there can be a large number of them.
        if LinkableElementProperty.METRIC not in element_filter.without_any_of:
            metrics_linked_to_semantic_model = self.get_joinable_metrics_for_semantic_model(
                semantic_model=measure_semantic_model,
                using_join_path=SemanticModelJoinPath(left_semantic_model_reference=measure_semantic_model.reference),
            )
        else:
            metrics_linked_to_semantic_model = LinkableElementSet()

        metric_time_elements = self._linkable_spec_index.measure_to_metric_time_elements[measure_reference]
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
        return self._linkable_spec_index.no_metric_linkable_element_set.filter(element_filter)

    # TODO: the results of this method don't actually match what will be allowed for the metric. This method checks
    def get_linkable_elements_for_metrics(
        self,
        metric_references: Sequence[MetricReference],
        element_filter: LinkableElementFilter = LinkableElementFilter(),
    ) -> LinkableElementSet:
        """Gets the valid linkable elements that are common to all requested metrics."""
        linkable_element_sets = []
        for metric_reference in metric_references:
            element_sets = self._linkable_spec_index.metric_to_linkable_element_sets.get(metric_reference.element_name)
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
