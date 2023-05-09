from __future__ import annotations

import logging
import more_itertools
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Tuple, Sequence, Dict, List, Optional, FrozenSet

from dbt_semantic_interfaces.objects.semantic_model import SemanticModel
from dbt_semantic_interfaces.objects.elements.dimension import DimensionType, Dimension
from dbt_semantic_interfaces.objects.elements.entity import EntityType
from dbt_semantic_interfaces.objects.user_configured_model import UserConfiguredModel
from dbt_semantic_interfaces.references import SemanticModelReference, MeasureReference, MetricReference
from metricflow.model.semantics.linkable_element_properties import LinkableElementProperties
from metricflow.model.semantics.semantic_model_join_evaluator import DataSourceJoinEvaluator
from dbt_semantic_interfaces.pretty_print import pformat_big_objects
from metricflow.protocols.semantics import DataSourceSemanticsAccessor
from metricflow.specs import (
    DEFAULT_TIME_GRANULARITY,
    LinkableSpecSet,
    DimensionSpec,
    TimeDimensionSpec,
    EntitySpec,
    EntityReference,
)
from dbt_semantic_interfaces.objects.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JoinPathKey:
    """A key that can uniquely identify an element and the joins used to realize the element."""

    element_name: str
    entity_links: Tuple[str, ...]
    time_granularity: Optional[TimeGranularity]


@dataclass(frozen=True)
class LinkableDimension:
    """Describes how a dimension can be realized by joining based on entity links."""

    element_name: str
    entity_links: Tuple[str, ...]
    properties: FrozenSet[LinkableElementProperties]
    time_granularity: Optional[TimeGranularity] = None

    @property
    def path_key(self) -> JoinPathKey:  # noqa: D
        return JoinPathKey(
            element_name=self.element_name,
            entity_links=self.entity_links,
            time_granularity=self.time_granularity,
        )

    @property
    def after_intersection(self) -> LinkableDimension:  # noqa: D
        return LinkableDimension(
            element_name=self.element_name,
            entity_links=self.entity_links,
            properties=frozenset({LinkableElementProperties.INTERSECTED}),
            time_granularity=self.time_granularity,
        )


@dataclass(frozen=True)
class LinkableEntity:
    """Describes how an entity can be realized by joining based on entity links."""

    element_name: str
    properties: FrozenSet[LinkableElementProperties]
    entity_links: Tuple[str, ...]

    @property
    def path_key(self) -> JoinPathKey:  # noqa: D
        return JoinPathKey(element_name=self.element_name, entity_links=self.entity_links, time_granularity=None)

    @property
    def after_intersection(self) -> LinkableEntity:  # noqa: D
        return LinkableEntity(
            element_name=self.element_name,
            entity_links=self.entity_links,
            properties=frozenset({LinkableElementProperties.INTERSECTED}),
        )


@dataclass(frozen=True)
class LinkableElementSet:
    """Container class for storing all linkable elements for a metric."""

    linkable_dimensions: Tuple[LinkableDimension, ...]
    linkable_entities: Tuple[LinkableEntity, ...]

    # Ambiguous elements are ones where there are multiple join paths through different data sources that can be taken
    # to get the element. This currently represents an error when defining data sources.
    ambiguous_linkable_dimensions: Tuple[LinkableDimension, ...]
    ambiguous_linkable_entities: Tuple[LinkableEntity, ...]

    @staticmethod
    def merge(linkable_element_sets: Sequence[LinkableElementSet]) -> LinkableElementSet:
        """Combine multiple sets together.

        If there are elements with the same join key, those elements will be categorized as ambiguous.
        """
        key_to_linkable_dimension: Dict[JoinPathKey, List[LinkableDimension]] = defaultdict(list)
        key_to_linkable_entity: Dict[JoinPathKey, List[LinkableEntity]] = defaultdict(list)

        for linkable_element_set in linkable_element_sets:
            for linkable_dimension in linkable_element_set.linkable_dimensions:
                key_to_linkable_dimension[linkable_dimension.path_key].append(linkable_dimension)
            for linkable_entity in linkable_element_set.linkable_entities:
                key_to_linkable_entity[linkable_entity.path_key].append(linkable_entity)

        linkable_dimensions = []
        linkable_entities = []

        ambiguous_linkable_dimensions = list(
            tuple(more_itertools.flatten([x.ambiguous_linkable_dimensions for x in linkable_element_sets]))
        )
        ambiguous_linkable_entities = list(
            tuple(more_itertools.flatten([x.ambiguous_linkable_entities for x in linkable_element_sets]))
        )

        for _, grouped_linkable_dimensions in key_to_linkable_dimension.items():
            for linkable_dimension in grouped_linkable_dimensions:
                if len(grouped_linkable_dimensions) == 1:
                    linkable_dimensions.append(linkable_dimension)
                else:
                    ambiguous_linkable_dimensions.append(linkable_dimension)

        for _, grouped_linkable_entity in key_to_linkable_entity.items():
            for linkable_entity in grouped_linkable_entity:
                if len(grouped_linkable_entity) == 1:
                    linkable_entities.append(linkable_entity)
                else:
                    ambiguous_linkable_entities.append(linkable_entity)

        return LinkableElementSet(
            linkable_dimensions=tuple(linkable_dimensions),
            linkable_entities=tuple(linkable_entities),
            ambiguous_linkable_dimensions=tuple(ambiguous_linkable_dimensions),
            ambiguous_linkable_entities=tuple(ambiguous_linkable_entities),
        )

    @staticmethod
    def intersection(linkable_element_sets: Sequence[LinkableElementSet]) -> LinkableElementSet:
        """Find the intersection of all elements in the sets."""
        if len(linkable_element_sets) == 0:
            return LinkableElementSet(
                linkable_dimensions=(),
                linkable_entities=(),
                ambiguous_linkable_dimensions=(),
                ambiguous_linkable_entities=(),
            )

        # Need to find an easier syntax for finding the common items from multiple LinkableElementSets.
        common_linkable_dimensions = (
            set.intersection(
                *[set([y.after_intersection for y in x.linkable_dimensions]) for x in linkable_element_sets]
            )
            if len(linkable_element_sets) > 0
            else set()
        )

        common_linkable_entities = (
            set.intersection(*[set([y.after_intersection for y in x.linkable_entities]) for x in linkable_element_sets])
            if len(linkable_element_sets) > 0
            else set()
        )

        common_ambiguous_linkable_dimensions = (
            set.intersection(
                *[set([y.after_intersection for y in x.ambiguous_linkable_dimensions]) for x in linkable_element_sets]
            )
            if len(linkable_element_sets) > 0
            else set()
        )

        common_ambiguous_linkable_entities = (
            set.intersection(
                *[set([y.after_intersection for y in x.ambiguous_linkable_entities]) for x in linkable_element_sets]
            )
            if len(linkable_element_sets) > 0
            else set()
        )

        return LinkableElementSet(
            linkable_dimensions=tuple(common_linkable_dimensions),
            linkable_entities=tuple(common_linkable_entities),
            ambiguous_linkable_dimensions=tuple(common_ambiguous_linkable_dimensions),
            ambiguous_linkable_entities=tuple(common_ambiguous_linkable_entities),
        )

    def filter(
        self, with_any_of: FrozenSet[LinkableElementProperties], without_any_of: FrozenSet[LinkableElementProperties]
    ) -> LinkableElementSet:
        """Filter elements in the set.

        First, only elements with at least one property in the "with_any_of" set are retained. Then, any elements with
        a property in "without_any_of" set are removed.
        """
        linkable_dimensions = tuple(
            x
            for x in self.linkable_dimensions
            if len(x.properties.intersection(with_any_of)) > 0 and len(x.properties.intersection(without_any_of)) == 0
        )

        linkable_entities = tuple(
            x
            for x in self.linkable_entities
            if len(x.properties.intersection(with_any_of)) > 0 and len(x.properties.intersection(without_any_of)) == 0
        )

        ambiguous_linkable_dimensions = tuple(
            x
            for x in self.ambiguous_linkable_dimensions
            if len(x.properties.intersection(with_any_of)) > 0 and len(x.properties.intersection(without_any_of)) == 0
        )

        ambiguous_linkable_entities = tuple(
            x
            for x in self.ambiguous_linkable_entities
            if len(x.properties.intersection(with_any_of)) > 0 and len(x.properties.intersection(without_any_of)) == 0
        )

        return LinkableElementSet(
            linkable_dimensions=linkable_dimensions,
            linkable_entities=linkable_entities,
            ambiguous_linkable_dimensions=ambiguous_linkable_dimensions,
            ambiguous_linkable_entities=ambiguous_linkable_entities,
        )

    @property
    def as_spec_set(self) -> LinkableSpecSet:  # noqa: D
        return LinkableSpecSet(
            dimension_specs=tuple(
                DimensionSpec(
                    element_name=x.element_name,
                    entity_links=tuple(EntityReference(element_name=x) for x in x.entity_links),
                )
                for x in self.linkable_dimensions
                if not x.time_granularity
            ),
            time_dimension_specs=tuple(
                TimeDimensionSpec(
                    element_name=x.element_name,
                    entity_links=tuple(EntityReference(element_name=x) for x in x.entity_links),
                    time_granularity=x.time_granularity,
                )
                for x in self.linkable_dimensions
                if x.time_granularity
            ),
            entity_specs=tuple(
                EntitySpec(
                    element_name=x.element_name,
                    entity_links=tuple(EntityReference(element_name=x) for x in x.entity_links),
                )
                for x in self.linkable_entities
            ),
        )


@dataclass(frozen=True)
class DataSourceJoinPathElement:
    """Describes joining a data source by the given entity."""

    data_source: SemanticModel
    join_on_entity: str


def _generate_linkable_time_dimensions(
    dimension: Dimension, entity_links: Tuple[str, ...], with_properties: FrozenSet[LinkableElementProperties]
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
                element_name=dimension.reference.element_name,
                entity_links=entity_links,
                time_granularity=time_granularity,
                properties=frozenset(properties),
            )
        )

    return linkable_dimensions


@dataclass(frozen=True)
class DataSourceJoinPath:
    """Describes a series of joins between the measure data source, and other data sources by entity.

    For example:

    (measure_source JOIN dimension_source0 ON entity A) JOIN dimension_source1 ON entity B

    would be represented by 2 path elements [(data_source0, A), (dimension_source1, B)]
    """

    path_elements: Tuple[DataSourceJoinPathElement, ...]

    def create_linkable_element_set(self, with_properties: FrozenSet[LinkableElementProperties]) -> LinkableElementSet:
        """Given the current path, generate the respective linkable elements from the last data source in the path."""
        entity_links = tuple(x.join_on_entity for x in self.path_elements)

        assert len(self.path_elements) > 0
        data_source = self.path_elements[-1].data_source

        linkable_dimensions = []
        linkable_entities = []

        for dimension in data_source.dimensions:
            dimension_type = dimension.type
            if dimension_type == DimensionType.CATEGORICAL:
                linkable_dimensions.append(
                    LinkableDimension(
                        element_name=dimension.reference.element_name,
                        entity_links=entity_links,
                        properties=with_properties,
                    )
                )
            elif dimension_type == DimensionType.TIME:
                linkable_dimensions.extend(
                    _generate_linkable_time_dimensions(
                        dimension=dimension,
                        entity_links=entity_links,
                        with_properties=with_properties,
                    )
                )
            else:
                raise RuntimeError(f"Unhandled type: {dimension_type}")

        for entity in data_source.entities:
            # Avoid creating "booking_id__booking_id"
            if entity.reference.element_name != entity_links[-1]:
                linkable_entities.append(
                    LinkableEntity(
                        element_name=entity.reference.element_name,
                        entity_links=entity_links,
                        properties=with_properties.union({LinkableElementProperties.ENTITY}),
                    )
                )

        return LinkableElementSet(
            linkable_dimensions=tuple(linkable_dimensions),
            linkable_entities=tuple(linkable_entities),
            ambiguous_linkable_dimensions=(),
            ambiguous_linkable_entities=(),
        )

    @property
    def last_semantic_model(self) -> SemanticModel:
        """The last data source that would be joined in this path."""
        assert len(self.path_elements) > 0
        return self.path_elements[-1].data_source


class ValidLinkableSpecResolver:
    """Figures out what linkable specs are valid for a given metric.

    e.g. Can you query the metric "bookings" by "listing__country_latest"?
    """

    def __init__(
        self,
        user_configured_model: UserConfiguredModel,
        semantic_model_semantics: DataSourceSemanticsAccessor,
        max_entity_links: int,
    ) -> None:
        """Constructor.

        Args:
            user_configured_model: the model to use.
            semantic_model_semantics: used to look up entities for a data source.
            max_entity_links: the maximum number of joins to do when computing valid elements.
        """
        self._user_configured_model = user_configured_model
        # Sort data sources by name for consistency in building derived objects.
        self._semantic_models = sorted(self._user_configured_model.data_sources, key=lambda x: x.name)
        self._join_evaluator = DataSourceJoinEvaluator(semantic_model_semantics)

        assert max_entity_links >= 0
        self._max_entity_links = max_entity_links

        # Map measures / entities to data sources that contain them.
        self._entity_to_semantic_model: Dict[str, List[SemanticModel]] = defaultdict(list)
        self._measure_to_semantic_model: Dict[str, List[SemanticModel]] = defaultdict(list)

        for data_source in self._semantic_models:
            for entity in data_source.entities:
                self._entity_to_semantic_model[entity.reference.element_name].append(data_source)

        self._metric_to_linkable_element_sets: Dict[str, List[LinkableElementSet]] = {}

        start_time = time.time()
        for metric in self._user_configured_model.metrics:
            linkable_sets_for_measure = []
            for measure in metric.measure_references:
                linkable_sets_for_measure.append(self._get_linkable_element_set_for_measure(measure))

            self._metric_to_linkable_element_sets[metric.name] = linkable_sets_for_measure
        logger.info(f"Building the [metric -> valid linkable element] index took: {time.time() - start_time:.2f}s")

    def _get_semantic_model_for_measure(self, measure_reference: MeasureReference) -> SemanticModel:  # noqa: D
        data_sources_where_measure_was_found = []
        for data_source in self._semantic_models:
            if any([x.reference.element_name == measure_reference.element_name for x in data_source.measures]):
                data_sources_where_measure_was_found.append(data_source)

        if len(data_sources_where_measure_was_found) == 0:
            raise ValueError(f"No data sources were found with {measure_reference} in the model")
        elif len(data_sources_where_measure_was_found) > 1:
            raise ValueError(
                f"Measure {measure_reference} was found in multiple data sources:\n"
                f"{pformat_big_objects(data_sources_where_measure_was_found)}"
            )
        return data_sources_where_measure_was_found[0]

    def _get_local_set(self, data_source: SemanticModel) -> LinkableElementSet:
        """Gets the local elements for a given data source."""
        linkable_dimensions = []
        linkable_entities = []

        for dimension in data_source.dimensions:
            dimension_type = dimension.type
            if dimension_type == DimensionType.CATEGORICAL:
                linkable_dimensions.append(
                    LinkableDimension(
                        element_name=dimension.reference.element_name,
                        entity_links=(),
                        properties=frozenset({LinkableElementProperties.LOCAL}),
                    )
                )
            elif dimension_type == DimensionType.TIME:
                linkable_dimensions.extend(
                    _generate_linkable_time_dimensions(
                        dimension=dimension,
                        entity_links=(),
                        with_properties=frozenset({LinkableElementProperties.LOCAL}),
                    )
                )
            else:
                raise RuntimeError(f"Unhandled type: {dimension_type}")

        additional_linkable_dimensions = []
        for entity in data_source.entities:
            linkable_entities.append(
                LinkableEntity(
                    element_name=entity.reference.element_name,
                    entity_links=(),
                    properties=frozenset({LinkableElementProperties.LOCAL, LinkableElementProperties.ENTITY}),
                )
            )
            # If a data source has a primary entity, we allow users to query using the dundered syntax, even though
            # there is no join involved. e.g. in the test model, the "listings_latest" data source would allow using
            # "listing__country_latest" for the "listings" metric.
            if entity.type == EntityType.PRIMARY:
                for linkable_dimension in linkable_dimensions:
                    properties = {LinkableElementProperties.LOCAL, LinkableElementProperties.LOCAL_LINKED}
                    additional_linkable_dimensions.append(
                        LinkableDimension(
                            element_name=linkable_dimension.element_name,
                            entity_links=(entity.reference.element_name,),
                            time_granularity=linkable_dimension.time_granularity,
                            properties=frozenset(linkable_dimension.properties.union(properties)),
                        )
                    )

        return LinkableElementSet(
            linkable_dimensions=tuple(linkable_dimensions + additional_linkable_dimensions),
            linkable_entities=tuple(linkable_entities),
            ambiguous_linkable_dimensions=(),
            ambiguous_linkable_entities=(),
        )

    def _get_semantic_models_with_joinable_entity(
        self,
        left_semantic_model_reference: SemanticModelReference,
        entity_reference: EntityReference,
    ) -> Sequence[SemanticModel]:
        # May switch to non-cached implementation.
        data_sources = self._entity_to_semantic_model[entity_reference.element_name]
        valid_semantic_models = []
        for data_source in data_sources:
            if self._join_evaluator.is_valid_semantic_model_join(
                left_semantic_model_reference=left_semantic_model_reference,
                right_semantic_model_reference=data_source.reference,
                on_entity_reference=entity_reference,
            ):
                valid_semantic_models.append(data_source)
        return valid_semantic_models

    def _get_linkable_element_set_for_measure(self, measure_reference: MeasureReference) -> LinkableElementSet:
        """Get the valid linkable elements for the given measure."""
        measure_semantic_model = self._get_semantic_model_for_measure(measure_reference)

        # Create local elements
        local_linkable_elements = self._get_local_set(measure_semantic_model)
        join_paths = []

        # Create 1-hop elements
        for entity in measure_semantic_model.entities:
            data_sources = self._get_semantic_models_with_joinable_entity(
                left_semantic_model_reference=measure_semantic_model.reference,
                entity_reference=entity.reference,
            )
            for data_source in data_sources:
                if data_source.name == measure_semantic_model.name:
                    continue
                join_paths.append(
                    DataSourceJoinPath(
                        path_elements=(
                            DataSourceJoinPathElement(
                                data_source=data_source, join_on_entity=entity.reference.element_name
                            ),
                        )
                    )
                )
        all_linkable_elements = LinkableElementSet.merge(
            [local_linkable_elements]
            + [
                x.create_linkable_element_set(with_properties=frozenset({LinkableElementProperties.JOINED}))
                for x in join_paths
            ]
        )

        # Create multi-hop elements. At each iteration, we generate the list of valid elements based on the current join
        # path, extend all paths to include the next valid data source, then repeat.
        for i in range(self._max_entity_links - 1):
            new_join_paths: List[DataSourceJoinPath] = []
            for join_path in join_paths:
                new_join_paths.extend(
                    self._find_next_possible_paths(
                        measure_semantic_model=measure_semantic_model, current_join_path=join_path
                    )
                )

            if len(new_join_paths) == 0:
                return all_linkable_elements

            all_linkable_elements = LinkableElementSet.merge(
                [all_linkable_elements]
                + [
                    x.create_linkable_element_set(
                        with_properties=frozenset(
                            {LinkableElementProperties.JOINED, LinkableElementProperties.MULTI_HOP}
                        )
                    )
                    for x in new_join_paths
                ]
            )
            join_paths = new_join_paths
        return all_linkable_elements

    def get_linkable_elements_for_metrics(
        self,
        metric_references: Sequence[MetricReference],
        with_any_of: FrozenSet[LinkableElementProperties],
        without_any_of: FrozenSet[LinkableElementProperties],
    ) -> LinkableElementSet:
        """Gets the valid linkable elements that are common to all requested metrics."""
        linkable_element_sets = []
        for metric_reference in metric_references:
            element_sets = self._metric_to_linkable_element_sets.get(metric_reference.element_name)
            if not element_sets:
                raise ValueError(f"Unknown metric: {metric_reference} in element set")
            metric_result = LinkableElementSet.intersection(
                [x.filter(with_any_of=with_any_of, without_any_of=without_any_of) for x in element_sets]
            )
            linkable_element_sets.append(metric_result)

        return LinkableElementSet.intersection(linkable_element_sets)

    def _find_next_possible_paths(
        self, measure_semantic_model: SemanticModel, current_join_path: DataSourceJoinPath
    ) -> Sequence[DataSourceJoinPath]:
        """Generate the set of possible paths that are 1 data source join longer that the "current_join_path"."""
        last_semantic_model_in_path = current_join_path.last_semantic_model
        new_join_paths = []

        for entity in last_semantic_model_in_path.entities:
            entity_name = entity.reference.element_name

            # Don't create cycles in the join path by joining on the same entity.
            if entity_name in set(x.join_on_entity for x in current_join_path.path_elements):
                continue

            data_sources_that_can_be_joined = self._get_semantic_models_with_joinable_entity(
                left_semantic_model_reference=last_semantic_model_in_path.reference,
                entity_reference=entity.reference,
            )
            for data_source in data_sources_that_can_be_joined:
                # Don't create cycles in the join path by repeating a data source in the path.
                if data_source.name == measure_semantic_model.name or any(
                    tuple(x.data_source.name == data_source.name for x in current_join_path.path_elements)
                ):
                    continue

                new_join_path = DataSourceJoinPath(
                    path_elements=current_join_path.path_elements
                    + (DataSourceJoinPathElement(data_source=data_source, join_on_entity=entity_name),)
                )
                new_join_paths.append(new_join_path)

        return new_join_paths
