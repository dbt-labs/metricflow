from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, FrozenSet, List, Optional, Sequence, Set, Tuple

from dbt_semantic_interfaces.pretty_print import pformat_big_objects
from dbt_semantic_interfaces.protocols.dimension import Dimension, DimensionType
from dbt_semantic_interfaces.protocols.entity import EntityType
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.protocols.semantic_model import SemanticModel
from dbt_semantic_interfaces.references import MeasureReference, MetricReference, SemanticModelReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.model.semantics.linkable_element_properties import LinkableElementProperties
from metricflow.model.semantics.semantic_model_join_evaluator import SemanticModelJoinEvaluator
from metricflow.protocols.semantics import SemanticModelAccessor
from metricflow.specs.specs import (
    DEFAULT_TIME_GRANULARITY,
    DimensionSpec,
    EntityReference,
    EntitySpec,
    LinkableSpecSet,
    TimeDimensionSpec,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ElementPathKey:
    """A key that can uniquely identify an element and the joins used to realize the element."""

    element_name: str
    entity_links: Tuple[str, ...]
    time_granularity: Optional[TimeGranularity]


@dataclass(frozen=True)
class LinkableDimension:
    """Describes how a dimension can be realized by joining based on entity links."""

    # The semantic model where this dimension was defined.
    semantic_model_origin: SemanticModelReference
    element_name: str
    entity_links: Tuple[str, ...]
    join_path: Tuple[SemanticModelJoinPathElement, ...]
    properties: FrozenSet[LinkableElementProperties]
    time_granularity: Optional[TimeGranularity] = None

    @property
    def path_key(self) -> ElementPathKey:  # noqa: D
        return ElementPathKey(
            element_name=self.element_name,
            entity_links=self.entity_links,
            time_granularity=self.time_granularity,
        )


@dataclass(frozen=True)
class LinkableEntity:
    """Describes how an entity can be realized by joining based on entity links."""

    # The semantic model where this entity was defined.
    semantic_model_origin: SemanticModelReference
    element_name: str
    properties: FrozenSet[LinkableElementProperties]
    entity_links: Tuple[str, ...]
    join_path: Tuple[SemanticModelJoinPathElement, ...]

    @property
    def path_key(self) -> ElementPathKey:  # noqa: D
        return ElementPathKey(element_name=self.element_name, entity_links=self.entity_links, time_granularity=None)


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
    path_key_to_linkable_dimensions: Dict[ElementPathKey, Tuple[LinkableDimension, ...]]
    path_key_to_linkable_entities: Dict[ElementPathKey, Tuple[LinkableEntity, ...]]

    @staticmethod
    def merge_by_path_key(linkable_element_sets: Sequence[LinkableElementSet]) -> LinkableElementSet:
        """Combine multiple sets together by the path key.

        If there are elements with the same join key, those elements will be categorized as ambiguous.
        """
        key_to_linkable_dimensions: Dict[ElementPathKey, List[LinkableDimension]] = defaultdict(list)
        key_to_linkable_entities: Dict[ElementPathKey, List[LinkableEntity]] = defaultdict(list)

        for linkable_element_set in linkable_element_sets:
            for path_key, linkable_dimensions in linkable_element_set.path_key_to_linkable_dimensions.items():
                key_to_linkable_dimensions[path_key].extend(linkable_dimensions)
            for path_key, linkable_entities in linkable_element_set.path_key_to_linkable_entities.items():
                key_to_linkable_entities[path_key].extend(linkable_entities)

        # Convert the dictionaries to use tuples instead of lists.
        return LinkableElementSet(
            path_key_to_linkable_dimensions={
                path_key: tuple(dimensions) for path_key, dimensions in key_to_linkable_dimensions.items()
            },
            path_key_to_linkable_entities={
                path_key: tuple(entities) for path_key, entities in key_to_linkable_entities.items()
            },
        )

    @staticmethod
    def intersection_by_path_key(linkable_element_sets: Sequence[LinkableElementSet]) -> LinkableElementSet:
        """Find the intersection of all elements in the sets by path key.

        This is useful to figure out the common dimensions that are possible to query with multiple metrics. You would
        find the LinakbleSpecSet for each metric in the query, then do an intersection of the sets.
        """
        if len(linkable_element_sets) == 0:
            return LinkableElementSet(
                path_key_to_linkable_dimensions={},
                path_key_to_linkable_entities={},
            )

        # Find path keys that are common to all LinkableElementSets.
        common_linkable_dimension_path_keys: Set[ElementPathKey] = set.intersection(
            *[
                set(linkable_element_set.path_key_to_linkable_dimensions.keys())
                for linkable_element_set in linkable_element_sets
            ]
        )

        common_linkable_entity_path_keys: Set[ElementPathKey] = set.intersection(
            *[
                set(linkable_element_set.path_key_to_linkable_entities.keys())
                for linkable_element_set in linkable_element_sets
            ]
        )

        # Create a new LinkableElementSet that only includes items where the path key is common to all sets.
        join_path_to_linkable_dimensions: Dict[ElementPathKey, Set[LinkableDimension]] = defaultdict(set)
        join_path_to_linkable_entities: Dict[ElementPathKey, Set[LinkableEntity]] = defaultdict(set)

        for linkable_element_set in linkable_element_sets:
            for path_key, linkable_dimensions in linkable_element_set.path_key_to_linkable_dimensions.items():
                if path_key in common_linkable_dimension_path_keys:
                    join_path_to_linkable_dimensions[path_key].update(linkable_dimensions)
            for path_key, linkable_entities in linkable_element_set.path_key_to_linkable_entities.items():
                if path_key in common_linkable_entity_path_keys:
                    join_path_to_linkable_entities[path_key].update(linkable_entities)

        return LinkableElementSet(
            path_key_to_linkable_dimensions={
                path_key: tuple(
                    sorted(
                        dimensions,
                        key=lambda linkable_dimension: linkable_dimension.semantic_model_origin.semantic_model_name,
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
        )

    def filter(
        self, with_any_of: FrozenSet[LinkableElementProperties], without_any_of: FrozenSet[LinkableElementProperties]
    ) -> LinkableElementSet:
        """Filter elements in the set.

        First, only elements with at least one property in the "with_any_of" set are retained. Then, any elements with
        a property in "without_any_of" set are removed.
        """
        key_to_linkable_dimensions: Dict[ElementPathKey, Tuple[LinkableDimension, ...]] = {}
        key_to_linkable_entities: Dict[ElementPathKey, Tuple[LinkableEntity, ...]] = {}

        for path_key, linkable_dimensions in self.path_key_to_linkable_dimensions.items():
            filtered_linkable_dimensions = tuple(
                linkable_dimension
                for linkable_dimension in linkable_dimensions
                if len(linkable_dimension.properties.intersection(with_any_of)) > 0
                and len(linkable_dimension.properties.intersection(without_any_of)) == 0
            )
            if len(filtered_linkable_dimensions) > 0:
                key_to_linkable_dimensions[path_key] = filtered_linkable_dimensions

        for path_key, linkable_entities in self.path_key_to_linkable_entities.items():
            filtered_linkable_entities = tuple(
                linkable_entity
                for linkable_entity in linkable_entities
                if len(linkable_entity.properties.intersection(with_any_of)) > 0
                and len(linkable_entity.properties.intersection(without_any_of)) == 0
            )
            if len(filtered_linkable_entities) > 0:
                key_to_linkable_entities[path_key] = filtered_linkable_entities

        return LinkableElementSet(
            path_key_to_linkable_dimensions=key_to_linkable_dimensions,
            path_key_to_linkable_entities=key_to_linkable_entities,
        )

    @property
    def as_spec_set(self) -> LinkableSpecSet:  # noqa: D
        return LinkableSpecSet(
            dimension_specs=tuple(
                DimensionSpec(
                    element_name=path_key.element_name,
                    entity_links=tuple(EntityReference(element_name=x) for x in path_key.entity_links),
                )
                for path_key in self.path_key_to_linkable_dimensions.keys()
                if not path_key.time_granularity
            ),
            time_dimension_specs=tuple(
                TimeDimensionSpec(
                    element_name=path_key.element_name,
                    entity_links=tuple(EntityReference(element_name=x) for x in path_key.entity_links),
                    time_granularity=path_key.time_granularity,
                )
                for path_key in self.path_key_to_linkable_dimensions.keys()
                if path_key.time_granularity
            ),
            entity_specs=tuple(
                EntitySpec(
                    element_name=path_key.element_name,
                    entity_links=tuple(EntityReference(element_name=x) for x in path_key.entity_links),
                )
                for path_key in self.path_key_to_linkable_entities
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
        )


@dataclass(frozen=True)
class SemanticModelJoinPathElement:
    """Describes joining a semantic model by the given entity."""

    semantic_model_reference: SemanticModelReference
    join_on_entity: str


def _generate_linkable_time_dimensions(
    semantic_model_origin: SemanticModelReference,
    dimension: Dimension,
    entity_links: Tuple[str, ...],
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

    def create_linkable_element_set(
        self, semantic_model_accessor: SemanticModelAccessor, with_properties: FrozenSet[LinkableElementProperties]
    ) -> LinkableElementSet:
        """Given the current path, generate the respective linkable elements from the last semantic model in the path."""
        entity_links = tuple(x.join_on_entity for x in self.path_elements)

        assert len(self.path_elements) > 0
        semantic_model = semantic_model_accessor.get_by_reference(self.path_elements[-1].semantic_model_reference)
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
                        entity_links=entity_links,
                        join_path=self.path_elements,
                        properties=with_properties,
                    )
                )
            elif dimension_type == DimensionType.TIME:
                linkable_dimensions.extend(
                    _generate_linkable_time_dimensions(
                        semantic_model_origin=semantic_model.reference,
                        dimension=dimension,
                        entity_links=entity_links,
                        join_path=(),
                        with_properties=with_properties,
                    )
                )
            else:
                raise RuntimeError(f"Unhandled type: {dimension_type}")

        for entity in semantic_model.entities:
            # Avoid creating "booking_id__booking_id"
            if entity.reference.element_name != entity_links[-1]:
                linkable_entities.append(
                    LinkableEntity(
                        semantic_model_origin=semantic_model.reference,
                        element_name=entity.reference.element_name,
                        entity_links=entity_links,
                        join_path=self.path_elements,
                        properties=with_properties.union({LinkableElementProperties.ENTITY}),
                    )
                )

        return LinkableElementSet(
            path_key_to_linkable_dimensions={
                linkable_dimension.path_key: (linkable_dimension,) for linkable_dimension in linkable_dimensions
            },
            path_key_to_linkable_entities={
                linkable_entity.path_key: (linkable_entity,) for linkable_entity in linkable_entities
            },
        )

    @property
    def last_semantic_model_reference(self) -> SemanticModelReference:
        """The last semantic model that would be joined in this path."""
        assert len(self.path_elements) > 0
        return self.path_elements[-1].semantic_model_reference


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

        start_time = time.time()
        for metric in self._semantic_manifest.metrics:
            linkable_sets_for_measure = []
            for measure in metric.measure_references:
                linkable_sets_for_measure.append(self._get_linkable_element_set_for_measure(measure))

            self._metric_to_linkable_element_sets[metric.name] = linkable_sets_for_measure
        logger.info(f"Building the [metric -> valid linkable element] index took: {time.time() - start_time:.2f}s")

    def _get_semantic_model_for_measure(self, measure_reference: MeasureReference) -> SemanticModel:  # noqa: D
        semantic_models_where_measure_was_found = []
        for semantic_model in self._semantic_models:
            if any([x.reference.element_name == measure_reference.element_name for x in semantic_model.measures]):
                semantic_models_where_measure_was_found.append(semantic_model)

        if len(semantic_models_where_measure_was_found) == 0:
            raise ValueError(f"No semantic models were found with {measure_reference} in the model")
        elif len(semantic_models_where_measure_was_found) > 1:
            raise ValueError(
                f"Measure {measure_reference} was found in multiple semantic models:\n"
                f"{pformat_big_objects(semantic_models_where_measure_was_found)}"
            )
        return semantic_models_where_measure_was_found[0]

    def _get_local_set(self, semantic_model: SemanticModel) -> LinkableElementSet:
        """Gets the local elements for a given semantic model."""
        linkable_dimensions = []
        linkable_entities = []

        for dimension in semantic_model.dimensions:
            dimension_type = dimension.type
            if dimension_type == DimensionType.CATEGORICAL:
                linkable_dimensions.append(
                    LinkableDimension(
                        semantic_model_origin=semantic_model.reference,
                        element_name=dimension.reference.element_name,
                        entity_links=(),
                        join_path=(),
                        properties=frozenset({LinkableElementProperties.LOCAL}),
                    )
                )
            elif dimension_type == DimensionType.TIME:
                linkable_dimensions.extend(
                    _generate_linkable_time_dimensions(
                        semantic_model_origin=semantic_model.reference,
                        dimension=dimension,
                        entity_links=(),
                        join_path=(),
                        with_properties=frozenset({LinkableElementProperties.LOCAL}),
                    )
                )
            else:
                raise RuntimeError(f"Unhandled type: {dimension_type}")

        additional_linkable_dimensions = []
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
            # If a semantic model has a primary entity, we allow users to query using the dundered syntax, even though
            # there is no join involved. e.g. in the test model, the "listings_latest" semantic model would allow using
            # "listing__country_latest" for the "listings" metric.
            if entity.type == EntityType.PRIMARY:
                for linkable_dimension in linkable_dimensions:
                    properties = {LinkableElementProperties.LOCAL, LinkableElementProperties.LOCAL_LINKED}
                    additional_linkable_dimensions.append(
                        LinkableDimension(
                            semantic_model_origin=semantic_model.reference,
                            element_name=linkable_dimension.element_name,
                            entity_links=(entity.reference.element_name,),
                            join_path=(),
                            time_granularity=linkable_dimension.time_granularity,
                            properties=frozenset(linkable_dimension.properties.union(properties)),
                        )
                    )

        return LinkableElementSet(
            path_key_to_linkable_dimensions={
                linkable_dimension.path_key: (linkable_dimension,)
                for linkable_dimension in tuple(linkable_dimensions + additional_linkable_dimensions)
            },
            path_key_to_linkable_entities={
                linkabe_entity.path_key: (linkabe_entity,) for linkabe_entity in tuple(linkable_entities)
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

    def _get_linkable_element_set_for_measure(self, measure_reference: MeasureReference) -> LinkableElementSet:
        """Get the valid linkable elements for the given measure."""
        measure_semantic_model = self._get_semantic_model_for_measure(measure_reference)

        # Create local elements
        local_linkable_elements = self._get_local_set(measure_semantic_model)
        join_paths = []

        # Create 1-hop elements
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
                                join_on_entity=entity.reference.element_name,
                            ),
                        )
                    )
                )
        all_linkable_elements = LinkableElementSet.merge_by_path_key(
            [local_linkable_elements]
            + [
                x.create_linkable_element_set(
                    semantic_model_accessor=self._semantic_model_lookup,
                    with_properties=frozenset({LinkableElementProperties.JOINED}),
                )
                for x in join_paths
            ]
        )

        # Create multi-hop elements. At each iteration, we generate the list of valid elements based on the current join
        # path, extend all paths to include the next valid semantic model, then repeat.
        for i in range(self._max_entity_links - 1):
            new_join_paths: List[SemanticModelJoinPath] = []
            for join_path in join_paths:
                new_join_paths.extend(
                    self._find_next_possible_paths(
                        measure_semantic_model=measure_semantic_model, current_join_path=join_path
                    )
                )

            if len(new_join_paths) == 0:
                return all_linkable_elements

            all_linkable_elements = LinkableElementSet.merge_by_path_key(
                [all_linkable_elements]
                + [
                    x.create_linkable_element_set(
                        semantic_model_accessor=self._semantic_model_lookup,
                        with_properties=frozenset(
                            {LinkableElementProperties.JOINED, LinkableElementProperties.MULTI_HOP}
                        ),
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
            entity_name = entity.reference.element_name

            # Don't create cycles in the join path by joining on the same entity.
            if entity_name in set(x.join_on_entity for x in current_join_path.path_elements):
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
                            semantic_model_reference=semantic_model.reference, join_on_entity=entity_name
                        ),
                    )
                )
                new_join_paths.append(new_join_path)

        return new_join_paths
