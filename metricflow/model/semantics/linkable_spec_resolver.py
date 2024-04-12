from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Sequence, Set, Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols.dimension import Dimension, DimensionType
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.protocols.semantic_model import SemanticModel
from dbt_semantic_interfaces.references import (
    DimensionReference,
    MeasureReference,
    MetricReference,
    SemanticModelReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums import MetricType
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.dataset.dataset import DataSet
from metricflow.errors.errors import UnknownMetricLinkingError
from metricflow.mf_logging.pretty_print import mf_pformat
from metricflow.model.semantics.linkable_element_properties import LinkableElementProperties
from metricflow.model.semantics.semantic_model_join_evaluator import SemanticModelJoinEvaluator
from metricflow.protocols.semantics import SemanticModelAccessor
from metricflow.specs.specs import (
    DEFAULT_TIME_GRANULARITY,
    DimensionSpec,
    EntityReference,
    EntitySpec,
    GroupByMetricSpec,
    LinkableSpecSet,
    TimeDimensionSpec,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ElementPathKey:
    """A key that can uniquely identify an element and the joins used to realize the element."""

    element_name: str
    entity_links: Tuple[EntityReference, ...]
    time_granularity: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None


@dataclass(frozen=True)
class LinkableDimension:
    """Describes how a dimension can be realized by joining based on entity links."""

    # The semantic model where this dimension was defined.
    semantic_model_origin: Optional[SemanticModelReference]
    element_name: str
    entity_links: Tuple[EntityReference, ...]
    join_path: Tuple[SemanticModelJoinPathElement, ...]
    properties: FrozenSet[LinkableElementProperties]
    time_granularity: Optional[TimeGranularity]
    date_part: Optional[DatePart]

    @property
    def path_key(self) -> ElementPathKey:  # noqa: D102
        return ElementPathKey(
            element_name=self.element_name,
            entity_links=self.entity_links,
            time_granularity=self.time_granularity,
            date_part=self.date_part,
        )

    @property
    def reference(self) -> DimensionReference:  # noqa: D102
        return DimensionReference(element_name=self.element_name)


@dataclass(frozen=True)
class LinkableEntity:
    """Describes how an entity can be realized by joining based on entity links."""

    # The semantic model where this entity was defined.
    semantic_model_origin: SemanticModelReference
    element_name: str
    properties: FrozenSet[LinkableElementProperties]
    entity_links: Tuple[EntityReference, ...]
    join_path: Tuple[SemanticModelJoinPathElement, ...]

    @property
    def path_key(self) -> ElementPathKey:  # noqa: D102
        return ElementPathKey(element_name=self.element_name, entity_links=self.entity_links)

    @property
    def reference(self) -> EntityReference:  # noqa: D102
        return EntityReference(element_name=self.element_name)


@dataclass(frozen=True)
class LinkableMetric:
    """Describes how a metric can be realized by joining based on entity links."""

    element_name: str
    join_by_semantic_model: SemanticModelReference
    # TODO: Enable joining by dimension
    entity_links: Tuple[EntityReference, ...]
    properties: FrozenSet[LinkableElementProperties]
    join_path: Tuple[SemanticModelJoinPathElement, ...]

    @property
    def path_key(self) -> ElementPathKey:  # noqa: D102
        return ElementPathKey(element_name=self.element_name, entity_links=self.entity_links)

    @property
    def reference(self) -> MetricReference:  # noqa: D102
        return MetricReference(element_name=self.element_name)


@dataclass(frozen=True)
class LinkableElementSet:
    """Container class for storing all linkable elements for a metric.

    TODO: There are similarities with LinkableSpecSet - consider consolidation.
    """

    # Dictionaries that map the path key to context on the dimension
    #
    # For example:
    # {
    #   "listing__country_latest": (
    #     LinkableDimension(
    #       element_name="country_latest",
    #       entity_links=("listing",),
    #       semantic_model_origin="listings_latest_source",
    #   )
    # }
    path_key_to_linkable_dimensions: Dict[ElementPathKey, Tuple[LinkableDimension, ...]] = field(default_factory=dict)
    path_key_to_linkable_entities: Dict[ElementPathKey, Tuple[LinkableEntity, ...]] = field(default_factory=dict)
    path_key_to_linkable_metrics: Dict[ElementPathKey, Tuple[LinkableMetric, ...]] = field(default_factory=dict)

    @staticmethod
    def merge_by_path_key(linkable_element_sets: Sequence[LinkableElementSet]) -> LinkableElementSet:
        """Combine multiple sets together by the path key.

        If there are elements with the same join key, those elements will be categorized as ambiguous.
        """
        key_to_linkable_dimensions: Dict[ElementPathKey, List[LinkableDimension]] = defaultdict(list)
        key_to_linkable_entities: Dict[ElementPathKey, List[LinkableEntity]] = defaultdict(list)
        key_to_linkable_metrics: Dict[ElementPathKey, List[LinkableMetric]] = defaultdict(list)

        for linkable_element_set in linkable_element_sets:
            for path_key, linkable_dimensions in linkable_element_set.path_key_to_linkable_dimensions.items():
                key_to_linkable_dimensions[path_key].extend(linkable_dimensions)
            for path_key, linkable_entities in linkable_element_set.path_key_to_linkable_entities.items():
                key_to_linkable_entities[path_key].extend(linkable_entities)
            for path_key, linkable_metrics in linkable_element_set.path_key_to_linkable_metrics.items():
                key_to_linkable_metrics[path_key].extend(linkable_metrics)

        # Convert the dictionaries to use tuples instead of lists.
        return LinkableElementSet(
            path_key_to_linkable_dimensions={
                path_key: tuple(dimensions) for path_key, dimensions in key_to_linkable_dimensions.items()
            },
            path_key_to_linkable_entities={
                path_key: tuple(entities) for path_key, entities in key_to_linkable_entities.items()
            },
            path_key_to_linkable_metrics={
                path_key: tuple(metrics) for path_key, metrics in key_to_linkable_metrics.items()
            },
        )

    @staticmethod
    def intersection_by_path_key(linkable_element_sets: Sequence[LinkableElementSet]) -> LinkableElementSet:
        """Find the intersection of all elements in the sets by path key.

        This is useful to figure out the common dimensions that are possible to query with multiple metrics. You would
        find the LinkableSpecSet for each metric in the query, then do an intersection of the sets.
        """
        if len(linkable_element_sets) == 0:
            return LinkableElementSet()

        # Find path keys that are common to all LinkableElementSets.
        dimension_path_keys: List[Set[ElementPathKey]] = []
        entity_path_keys: List[Set[ElementPathKey]] = []
        metric_path_keys: List[Set[ElementPathKey]] = []
        for linkable_element_set in linkable_element_sets:
            dimension_path_keys.append(set(linkable_element_set.path_key_to_linkable_dimensions.keys()))
            entity_path_keys.append(set(linkable_element_set.path_key_to_linkable_entities.keys()))
            metric_path_keys.append(set(linkable_element_set.path_key_to_linkable_metrics.keys()))
        common_linkable_dimension_path_keys = set.intersection(*dimension_path_keys) if dimension_path_keys else set()
        common_linkable_entity_path_keys = set.intersection(*entity_path_keys) if entity_path_keys else set()
        common_linkable_metric_path_keys = set.intersection(*metric_path_keys) if metric_path_keys else set()

        # Create a new LinkableElementSet that only includes items where the path key is common to all sets.
        join_path_to_linkable_dimensions: Dict[ElementPathKey, Set[LinkableDimension]] = defaultdict(set)
        join_path_to_linkable_entities: Dict[ElementPathKey, Set[LinkableEntity]] = defaultdict(set)
        join_path_to_linkable_metrics: Dict[ElementPathKey, Set[LinkableMetric]] = defaultdict(set)

        for linkable_element_set in linkable_element_sets:
            for path_key, linkable_dimensions in linkable_element_set.path_key_to_linkable_dimensions.items():
                if path_key in common_linkable_dimension_path_keys:
                    join_path_to_linkable_dimensions[path_key].update(linkable_dimensions)
            for path_key, linkable_entities in linkable_element_set.path_key_to_linkable_entities.items():
                if path_key in common_linkable_entity_path_keys:
                    join_path_to_linkable_entities[path_key].update(linkable_entities)
            for path_key, linkable_metrics in linkable_element_set.path_key_to_linkable_metrics.items():
                if path_key in common_linkable_metric_path_keys:
                    join_path_to_linkable_metrics[path_key].update(linkable_metrics)

        return LinkableElementSet(
            path_key_to_linkable_dimensions={
                path_key: tuple(
                    sorted(
                        dimensions,
                        key=lambda linkable_dimension: (
                            linkable_dimension.semantic_model_origin.semantic_model_name
                            if linkable_dimension.semantic_model_origin
                            else ""
                        ),
                    )
                )
                for path_key, dimensions in join_path_to_linkable_dimensions.items()
            },
            path_key_to_linkable_entities={
                path_key: tuple(
                    sorted(
                        entities, key=lambda linkable_entity: linkable_entity.semantic_model_origin.semantic_model_name
                    )
                )
                for path_key, entities in join_path_to_linkable_entities.items()
            },
            path_key_to_linkable_metrics={
                path_key: tuple(
                    sorted(
                        metrics, key=lambda linkable_metric: linkable_metric.join_by_semantic_model.semantic_model_name
                    )
                )
                for path_key, metrics in join_path_to_linkable_metrics.items()
            },
        )

    def filter(
        self,
        with_any_of: FrozenSet[LinkableElementProperties],
        without_any_of: FrozenSet[LinkableElementProperties] = frozenset(),
        without_all_of: FrozenSet[LinkableElementProperties] = frozenset(),
    ) -> LinkableElementSet:
        """Filter elements in the set.

        First, only elements with at least one property in the "with_any_of" set are retained. Then, any elements with
        a property in "without_any_of" set are removed. Lastly, any elements with all properties in without_all_of
        are removed.
        """
        key_to_linkable_dimensions: Dict[ElementPathKey, Tuple[LinkableDimension, ...]] = {}
        key_to_linkable_entities: Dict[ElementPathKey, Tuple[LinkableEntity, ...]] = {}
        key_to_linkable_metrics: Dict[ElementPathKey, Tuple[LinkableMetric, ...]] = {}

        for path_key, linkable_dimensions in self.path_key_to_linkable_dimensions.items():
            filtered_linkable_dimensions = tuple(
                linkable_dimension
                for linkable_dimension in linkable_dimensions
                if len(linkable_dimension.properties.intersection(with_any_of)) > 0
                and len(linkable_dimension.properties.intersection(without_any_of)) == 0
                and (
                    len(without_all_of) == 0
                    or linkable_dimension.properties.intersection(without_all_of) != without_all_of
                )
            )
            if len(filtered_linkable_dimensions) > 0:
                key_to_linkable_dimensions[path_key] = filtered_linkable_dimensions

        for path_key, linkable_entities in self.path_key_to_linkable_entities.items():
            filtered_linkable_entities = tuple(
                linkable_entity
                for linkable_entity in linkable_entities
                if len(linkable_entity.properties.intersection(with_any_of)) > 0
                and len(linkable_entity.properties.intersection(without_any_of)) == 0
                and (
                    len(without_all_of) == 0
                    or linkable_entity.properties.intersection(without_all_of) != without_all_of
                )
            )
            if len(filtered_linkable_entities) > 0:
                key_to_linkable_entities[path_key] = filtered_linkable_entities

        for path_key, linkable_metrics in self.path_key_to_linkable_metrics.items():
            filtered_linkable_metrics = tuple(
                linkable_metric
                for linkable_metric in linkable_metrics
                if len(linkable_metric.properties.intersection(with_any_of)) > 0
                and len(linkable_metric.properties.intersection(without_any_of)) == 0
                and (
                    len(without_all_of) == 0
                    or linkable_metric.properties.intersection(without_all_of) != without_all_of
                )
            )
            if len(filtered_linkable_metrics) > 0:
                key_to_linkable_metrics[path_key] = filtered_linkable_metrics

        return LinkableElementSet(
            path_key_to_linkable_dimensions=key_to_linkable_dimensions,
            path_key_to_linkable_entities=key_to_linkable_entities,
            path_key_to_linkable_metrics=key_to_linkable_metrics,
        )

    @property
    def as_spec_set(self) -> LinkableSpecSet:  # noqa: D102
        return LinkableSpecSet(
            dimension_specs=tuple(
                DimensionSpec(
                    element_name=path_key.element_name,
                    entity_links=path_key.entity_links,
                )
                for path_key in self.path_key_to_linkable_dimensions.keys()
                if not path_key.time_granularity
            ),
            time_dimension_specs=tuple(
                TimeDimensionSpec(
                    element_name=path_key.element_name,
                    entity_links=path_key.entity_links,
                    time_granularity=path_key.time_granularity,
                    date_part=path_key.date_part,
                )
                for path_key in self.path_key_to_linkable_dimensions.keys()
                if path_key.time_granularity
            ),
            entity_specs=tuple(
                EntitySpec(
                    element_name=path_key.element_name,
                    entity_links=path_key.entity_links,
                )
                for path_key in self.path_key_to_linkable_entities
            ),
            group_by_metric_specs=tuple(
                GroupByMetricSpec(
                    element_name=path_key.element_name,
                    entity_links=path_key.entity_links,
                )
                for path_key in self.path_key_to_linkable_metrics
            ),
        )

    @property
    def only_unique_path_keys(self) -> LinkableElementSet:
        """Returns a set that only includes path keys that map to a single element."""
        return LinkableElementSet(
            path_key_to_linkable_dimensions={
                path_key: linkable_dimensions
                for path_key, linkable_dimensions in self.path_key_to_linkable_dimensions.items()
                if len(linkable_dimensions) <= 1
            },
            path_key_to_linkable_entities={
                path_key: linkable_entities
                for path_key, linkable_entities in self.path_key_to_linkable_entities.items()
                if len(linkable_entities) <= 1
            },
            path_key_to_linkable_metrics={
                path_key: linkable_metrics
                for path_key, linkable_metrics in self.path_key_to_linkable_metrics.items()
                if len(linkable_metrics) <= 1
            },
        )


@dataclass(frozen=True)
class SemanticModelJoinPathElement:
    """Describes joining a semantic model by the given entity."""

    semantic_model_reference: SemanticModelReference
    join_on_entity: EntityReference


def _generate_linkable_time_dimensions(
    semantic_model_origin: SemanticModelReference,
    dimension: Dimension,
    entity_links: Tuple[EntityReference, ...],
    join_path: Sequence[SemanticModelJoinPathElement],
    with_properties: FrozenSet[LinkableElementProperties],
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
            properties.add(LinkableElementProperties.DERIVED_TIME_GRANULARITY)

        linkable_dimensions.append(
            LinkableDimension(
                semantic_model_origin=semantic_model_origin,
                element_name=dimension.reference.element_name,
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
                        entity_links=entity_links,
                        join_path=tuple(join_path),
                        time_granularity=time_granularity,
                        date_part=date_part,
                        properties=frozenset(properties),
                    )
                )

    return linkable_dimensions


@dataclass(frozen=True)
class SemanticModelJoinPath:
    """Describes a series of joins between the measure semantic model, and other semantic models by entity.

    For example:

    (measure_source JOIN dimension_source0 ON entity A) JOIN dimension_source1 ON entity B

    would be represented by 2 path elements [(semantic_model0, A), (dimension_source1, B)]
    """

    path_elements: Tuple[SemanticModelJoinPathElement, ...]

    @property
    def last_path_element(self) -> SemanticModelJoinPathElement:  # noqa: D102
        assert len(self.path_elements) > 0
        return self.path_elements[-1]

    @property
    def last_semantic_model_reference(self) -> SemanticModelReference:
        """The last semantic model that would be joined in this path."""
        return self.last_path_element.semantic_model_reference

    @property
    def last_entity_link(self) -> EntityReference:  # noqa: D102
        return self.last_path_element.join_on_entity

    @property
    def entity_links(self) -> Tuple[EntityReference, ...]:  # noqa: D102
        return tuple(path_element.join_on_entity for path_element in self.path_elements)

    @staticmethod
    def from_single_element(
        semantic_model_reference: SemanticModelReference, join_on_entity: EntityReference
    ) -> SemanticModelJoinPath:
        """Build SemanticModelJoinPath with just one join path element."""
        return SemanticModelJoinPath(
            path_elements=(
                SemanticModelJoinPathElement(
                    semantic_model_reference=semantic_model_reference,
                    join_on_entity=join_on_entity,
                ),
            )
        )


class ValidLinkableSpecResolver:
    """Figures out what linkable specs are valid for a given metric.

    e.g. Can you query the metric "bookings" by "listing__country_latest"?
    """

    def __init__(
        self,
        semantic_manifest: SemanticManifest,
        semantic_model_lookup: SemanticModelAccessor,
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
        self._joinable_metrics_for_entities: Dict[EntityReference, Set[MetricReference]] = defaultdict(set)

        start_time = time.time()
        for metric in self._semantic_manifest.metrics:
            linkable_sets_for_measure = []
            for measure in metric.measure_references:
                # Cumulative metrics currently can't be queried by other time granularities.
                if metric.type is MetricType.CUMULATIVE:
                    linkable_sets_for_measure.append(
                        self._get_linkable_element_set_for_measure(measure).filter(
                            with_any_of=LinkableElementProperties.all_properties(),
                            # Use filter() here becasue `without_all_of` param is only available on that method.
                            without_all_of=frozenset(
                                {
                                    LinkableElementProperties.METRIC_TIME,
                                    LinkableElementProperties.DERIVED_TIME_GRANULARITY,
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
                    self._joinable_metrics_for_entities[linkable_entity.reference].add(metric_reference)

        # If no metrics are specified, the query interface supports querying distinct values for dimensions, entities,
        # and group by metrics.
        linkable_element_sets_for_no_metrics_queries: List[LinkableElementSet] = []
        for semantic_model in semantic_manifest.semantic_models:
            linkable_element_sets_for_no_metrics_queries.append(self._get_elements_in_semantic_model(semantic_model))
            path_key_to_linkable_metrics = {}
            for entity_reference, metric_references in self._joinable_metrics_for_entities.items():
                # LinkableMetrics can be joined on a given entity no matter which semantic model it's in.
                for metric_reference in metric_references:
                    path_key = ElementPathKey(
                        element_name=metric_reference.element_name, entity_links=(entity_reference,)
                    )
                    linkable_metrics = [
                        LinkableMetric(
                            element_name=metric_reference.element_name,
                            entity_links=(entity.reference,),
                            join_path=(
                                SemanticModelJoinPathElement(
                                    semantic_model_reference=semantic_model.reference,
                                    join_on_entity=entity_reference,
                                ),
                            ),
                            join_by_semantic_model=semantic_model.reference,
                            properties=frozenset({LinkableElementProperties.METRIC}),
                        )
                        for semantic_model in self._entity_to_semantic_model[entity_reference.element_name]
                    ]
                    path_key_to_linkable_metrics[path_key] = tuple(linkable_metrics)
            linkable_element_sets_for_no_metrics_queries.append(
                LinkableElementSet(path_key_to_linkable_metrics=path_key_to_linkable_metrics)
            )

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
        for entity_reference in [entity.reference for entity in semantic_model.entities]:
            for metric_ref in self._joinable_metrics_for_entities[entity_reference]:
                linkable_metrics.append(
                    LinkableMetric(
                        element_name=metric_ref.element_name,
                        join_by_semantic_model=semantic_model.reference,
                        entity_links=(entity_reference,),
                        properties=frozenset({LinkableElementProperties.METRIC}),
                        join_path=(
                            SemanticModelJoinPathElement(
                                semantic_model_reference=semantic_model.reference, join_on_entity=entity_reference
                            ),
                        ),
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
                    properties=frozenset({LinkableElementProperties.LOCAL, LinkableElementProperties.ENTITY}),
                )
            )
            for entity_link in self._semantic_model_lookup.entity_links_for_local_elements(semantic_model):
                linkable_entities.append(
                    LinkableEntity(
                        semantic_model_origin=semantic_model.reference,
                        element_name=entity.reference.element_name,
                        entity_links=(entity_link,),
                        join_path=(),
                        properties=frozenset({LinkableElementProperties.LOCAL, LinkableElementProperties.ENTITY}),
                    )
                )

        for entity_link in self._semantic_model_lookup.entity_links_for_local_elements(semantic_model):
            dimension_properties = frozenset({LinkableElementProperties.LOCAL})
            for dimension in semantic_model.dimensions:
                dimension_type = dimension.type
                if dimension_type is DimensionType.CATEGORICAL:
                    linkable_dimensions.append(
                        LinkableDimension(
                            semantic_model_origin=semantic_model.reference,
                            element_name=dimension.reference.element_name,
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

    def _get_semantic_models_joinable_to_entity(
        self,
        left_semantic_model_reference: SemanticModelReference,
        entity_reference: EntityReference,
    ) -> Tuple[Sequence[SemanticModel], Sequence[SemanticModel]]:
        """Find all semantic models that contain this entity, split into those with valid vs. invalid joins."""
        # May switch to non-cached implementation.
        semantic_models = self._entity_to_semantic_model[entity_reference.element_name]
        semantic_models_with_valid_join = []
        semantic_models_with_fan_out_join = []

        for semantic_model in semantic_models:
            if self._join_evaluator.is_valid_semantic_model_join(
                left_semantic_model_reference=left_semantic_model_reference,
                right_semantic_model_reference=semantic_model.reference,
                on_entity_reference=entity_reference,
            ):
                semantic_models_with_valid_join.append(semantic_model)
            else:
                semantic_models_with_fan_out_join.append(semantic_model)

        return semantic_models_with_valid_join, semantic_models_with_fan_out_join

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
                    element_name=DataSet.metric_time_dimension_name(),
                    entity_links=(),
                    time_granularity=time_granularity,
                    date_part=date_part,
                )
                path_key_to_linkable_dimensions[path_key].append(
                    LinkableDimension(
                        semantic_model_origin=measure_semantic_model.reference if measure_semantic_model else None,
                        element_name=DataSet.metric_time_dimension_name(),
                        entity_links=(),
                        join_path=(),
                        # Anything that's not at the base time granularity of the measure's aggregation time dimension
                        # should be considered derived.
                        properties=(
                            frozenset({LinkableElementProperties.METRIC_TIME})
                            if time_granularity is defined_granularity and date_part is None
                            else frozenset(
                                {
                                    LinkableElementProperties.METRIC_TIME,
                                    LinkableElementProperties.DERIVED_TIME_GRANULARITY,
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

    def _get_valid_single_hop_join_paths(self, semantic_model: SemanticModel) -> Sequence[SemanticModelJoinPath]:
        """Get all valid join paths to join other semantic models to this one."""
        valid_join_paths = []
        for entity in semantic_model.entities:
            semantic_models_with_valid_join, _ = self._get_semantic_models_joinable_to_entity(
                left_semantic_model_reference=semantic_model.reference,
                entity_reference=entity.reference,
            )
            valid_join_paths += [
                SemanticModelJoinPath.from_single_element(joinable_semantic_model.reference, entity.reference)
                for joinable_semantic_model in semantic_models_with_valid_join
                if joinable_semantic_model.name != semantic_model.name
            ]
        return valid_join_paths

    def _get_joined_elements(self, measure_semantic_model: SemanticModel) -> LinkableElementSet:
        """Get the elements that can be generated by joining other models to the given model."""
        # Create single-hop elements
        valid_join_paths = self._get_valid_single_hop_join_paths(measure_semantic_model)
        single_hop_elements = LinkableElementSet.merge_by_path_key(
            [
                self.create_linkable_element_set_from_join_path(
                    join_path=join_path,
                    with_properties=frozenset({LinkableElementProperties.JOINED}),
                )
                for join_path in valid_join_paths
            ]
        )

        # Create multi-hop elements. At each iteration, we generate the list of valid elements based on the current join
        # path, extend all paths to include the next valid semantic model, then repeat.
        multi_hop_elements = LinkableElementSet()

        for _ in range(self._max_entity_links - 1):
            new_valid_join_paths: List[SemanticModelJoinPath] = []
            for join_path in valid_join_paths:
                new_valid_join_paths.extend(
                    self._find_next_possible_paths(
                        measure_semantic_model=measure_semantic_model, current_join_path=join_path
                    )
                )
            if len(new_valid_join_paths) == 0:
                break

            multi_hop_elements = LinkableElementSet.merge_by_path_key(
                (multi_hop_elements,)
                + tuple(
                    self.create_linkable_element_set_from_join_path(
                        join_path=new_join_path,
                        with_properties=frozenset(
                            {LinkableElementProperties.JOINED, LinkableElementProperties.MULTI_HOP}
                        ),
                    )
                    for new_join_path in new_valid_join_paths
                )
            )
            valid_join_paths = new_valid_join_paths

        return LinkableElementSet.merge_by_path_key((single_hop_elements, multi_hop_elements))

    def _get_linkable_element_set_for_measure(
        self,
        measure_reference: MeasureReference,
        with_any_of: FrozenSet[LinkableElementProperties] = LinkableElementProperties.all_properties(),
        without_any_of: FrozenSet[LinkableElementProperties] = frozenset(),
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
        with_any_of: FrozenSet[LinkableElementProperties],
        without_any_of: FrozenSet[LinkableElementProperties],
    ) -> LinkableElementSet:
        """Get the valid linkable elements for the given measure."""
        return self._get_linkable_element_set_for_measure(
            measure_reference=measure_reference,
            with_any_of=with_any_of,
            without_any_of=without_any_of,
        )

    def get_linkable_elements_for_distinct_values_query(
        self,
        with_any_of: FrozenSet[LinkableElementProperties],
        without_any_of: FrozenSet[LinkableElementProperties],
    ) -> LinkableElementSet:
        """Returns queryable items for a distinct group-by-item values query.

        A distinct group-by-item values query does not include any metrics.
        """
        return self._no_metric_linkable_element_set.filter(with_any_of=with_any_of, without_any_of=without_any_of)

    def get_linkable_elements_for_metrics(
        self,
        metric_references: Sequence[MetricReference],
        with_any_of: FrozenSet[LinkableElementProperties] = LinkableElementProperties.all_properties(),
        without_any_of: FrozenSet[LinkableElementProperties] = frozenset(),
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
            if entity_reference in current_join_path.entity_links:
                continue

            semantic_models_that_can_be_joined, _ = self._get_semantic_models_joinable_to_entity(
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
        with_properties: FrozenSet[LinkableElementProperties],
    ) -> LinkableElementSet:
        """Given the current path, generate the respective linkable elements from the last semantic model in the path."""
        semantic_model = self._semantic_model_lookup.get_by_reference(join_path.last_semantic_model_reference)
        assert semantic_model

        linkable_dimensions: List[LinkableDimension] = []
        linkable_entities: List[LinkableEntity] = []

        for dimension in semantic_model.dimensions:
            dimension_type = dimension.type
            if dimension_type == DimensionType.CATEGORICAL:
                linkable_dimensions.append(
                    LinkableDimension(
                        semantic_model_origin=semantic_model.reference,
                        element_name=dimension.reference.element_name,
                        entity_links=join_path.entity_links,
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
                        entity_links=join_path.entity_links,
                        join_path=join_path.path_elements,  # why was there no join path?
                        with_properties=with_properties,
                    )
                )
            else:
                raise RuntimeError(f"Unhandled type: {dimension_type}")

        for entity in semantic_model.entities:
            # Avoid creating "booking_id__booking_id"
            # TODO: we are getting the above - figure out why
            if entity.reference != join_path.last_entity_link:
                linkable_entities.append(
                    LinkableEntity(
                        semantic_model_origin=semantic_model.reference,
                        element_name=entity.reference.element_name,
                        entity_links=join_path.entity_links,
                        join_path=join_path.path_elements,
                        properties=with_properties.union({LinkableElementProperties.ENTITY}),
                    )
                )

        linkable_metrics = self._create_linkable_metrics_from_join_path(
            join_path=join_path, with_properties=with_properties
        )

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

    def _create_linkable_metrics_from_join_path(
        self,
        join_path: SemanticModelJoinPath,
        with_properties: FrozenSet[LinkableElementProperties],
    ) -> Sequence[LinkableMetric]:
        return [
            LinkableMetric(
                element_name=metric_reference.element_name,
                entity_links=join_path.entity_links,
                properties=with_properties.union({LinkableElementProperties.METRIC}),
                join_path=join_path.path_elements,
                join_by_semantic_model=join_path.last_semantic_model_reference,
            )
            for metric_reference in self._joinable_metrics_for_entities[join_path.last_entity_link]
        ]
