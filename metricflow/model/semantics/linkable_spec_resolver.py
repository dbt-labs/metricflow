from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Tuple, Sequence, Dict, List, Optional, FrozenSet

from metricflow.instances import EntityReference
from dbt.contracts.graph.nodes import Entity
from dbt.contracts.graph.dimensions import DimensionType, Dimension
from dbt.contracts.graph.identifiers import IdentifierType
from dbt.contracts.graph.manifest import UserConfiguredModel
from metricflow.model.semantics.linkable_element_properties import LinkableElementProperties
from metricflow.model.semantics.entity_join_evaluator import EntityJoinEvaluator
from dbt.semantic.object_utils import pformat_big_objects, flatten_nested_sequence
from metricflow.protocols.semantics import EntitySemanticsAccessor
from dbt.semantic.references import MeasureReference
from dbt.semantic.references import MetricReference
from metricflow.specs import (
    DEFAULT_TIME_GRANULARITY,
    LinkableSpecSet,
    DimensionSpec,
    TimeDimensionSpec,
    IdentifierSpec,
    IdentifierReference,
)
from dbt.semantic.time import TimeGranularity

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JoinPathKey:
    """A key that can uniquely identify an element and the joins used to realize the element."""

    element_name: str
    identifier_links: Tuple[str, ...]
    time_granularity: Optional[TimeGranularity]


@dataclass(frozen=True)
class LinkableDimension:
    """Describes how a dimension can be realized by joining based on identifier links."""

    element_name: str
    identifier_links: Tuple[str, ...]
    properties: FrozenSet[LinkableElementProperties]
    time_granularity: Optional[TimeGranularity] = None

    @property
    def path_key(self) -> JoinPathKey:  # noqa: D
        return JoinPathKey(
            element_name=self.element_name,
            identifier_links=self.identifier_links,
            time_granularity=self.time_granularity,
        )

    @property
    def after_intersection(self) -> LinkableDimension:  # noqa: D
        return LinkableDimension(
            element_name=self.element_name,
            identifier_links=self.identifier_links,
            properties=frozenset({LinkableElementProperties.INTERSECTED}),
            time_granularity=self.time_granularity,
        )


@dataclass(frozen=True)
class LinkableIdentifier:
    """Describes how an identifier can be realized by joining based on identifier links."""

    element_name: str
    properties: FrozenSet[LinkableElementProperties]
    identifier_links: Tuple[str, ...]

    @property
    def path_key(self) -> JoinPathKey:  # noqa: D
        return JoinPathKey(
            element_name=self.element_name, identifier_links=self.identifier_links, time_granularity=None
        )

    @property
    def after_intersection(self) -> LinkableIdentifier:  # noqa: D
        return LinkableIdentifier(
            element_name=self.element_name,
            identifier_links=self.identifier_links,
            properties=frozenset({LinkableElementProperties.INTERSECTED}),
        )


@dataclass(frozen=True)
class LinkableElementSet:
    """Container class for storing all linkable elements for a metric."""

    linkable_dimensions: Tuple[LinkableDimension, ...]
    linkable_identifiers: Tuple[LinkableIdentifier, ...]

    # Ambiguous elements are ones where there are multiple join paths through different entities that can be taken
    # to get the element. This currently represents an error when defining entities.
    ambiguous_linkable_dimensions: Tuple[LinkableDimension, ...]
    ambiguous_linkable_identifiers: Tuple[LinkableIdentifier, ...]

    @staticmethod
    def merge(linkable_element_sets: Sequence[LinkableElementSet]) -> LinkableElementSet:
        """Combine multiple sets together.

        If there are elements with the same join key, those elements will be categorized as ambiguous.
        """
        key_to_linkable_dimension: Dict[JoinPathKey, List[LinkableDimension]] = defaultdict(list)
        key_to_linkable_identifier: Dict[JoinPathKey, List[LinkableIdentifier]] = defaultdict(list)

        for linkable_element_set in linkable_element_sets:
            for linkable_dimension in linkable_element_set.linkable_dimensions:
                key_to_linkable_dimension[linkable_dimension.path_key].append(linkable_dimension)
            for linkable_identifier in linkable_element_set.linkable_identifiers:
                key_to_linkable_identifier[linkable_identifier.path_key].append(linkable_identifier)

        linkable_dimensions = []
        linkable_identifiers = []

        ambiguous_linkable_dimensions = list(
            flatten_nested_sequence([x.ambiguous_linkable_dimensions for x in linkable_element_sets])
        )
        ambiguous_linkable_identifiers = list(
            flatten_nested_sequence([x.ambiguous_linkable_identifiers for x in linkable_element_sets])
        )

        for _, grouped_linkable_dimensions in key_to_linkable_dimension.items():
            for linkable_dimension in grouped_linkable_dimensions:
                if len(grouped_linkable_dimensions) == 1:
                    linkable_dimensions.append(linkable_dimension)
                else:
                    ambiguous_linkable_dimensions.append(linkable_dimension)

        for _, grouped_linkable_identifier in key_to_linkable_identifier.items():
            for linkable_identifier in grouped_linkable_identifier:
                if len(grouped_linkable_identifier) == 1:
                    linkable_identifiers.append(linkable_identifier)
                else:
                    ambiguous_linkable_identifiers.append(linkable_identifier)

        return LinkableElementSet(
            linkable_dimensions=tuple(linkable_dimensions),
            linkable_identifiers=tuple(linkable_identifiers),
            ambiguous_linkable_dimensions=tuple(ambiguous_linkable_dimensions),
            ambiguous_linkable_identifiers=tuple(ambiguous_linkable_identifiers),
        )

    @staticmethod
    def intersection(linkable_element_sets: Sequence[LinkableElementSet]) -> LinkableElementSet:
        """Find the intersection of all elements in the sets."""
        if len(linkable_element_sets) == 0:
            return LinkableElementSet(
                linkable_dimensions=(),
                linkable_identifiers=(),
                ambiguous_linkable_dimensions=(),
                ambiguous_linkable_identifiers=(),
            )

        # Need to find an easier syntax for finding the common items from multiple LinkableElementSets.
        common_linkable_dimensions = (
            set.intersection(
                *[set([y.after_intersection for y in x.linkable_dimensions]) for x in linkable_element_sets]
            )
            if len(linkable_element_sets) > 0
            else set()
        )

        common_linkable_identifiers = (
            set.intersection(
                *[set([y.after_intersection for y in x.linkable_identifiers]) for x in linkable_element_sets]
            )
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

        common_ambiguous_linkable_identifiers = (
            set.intersection(
                *[set([y.after_intersection for y in x.ambiguous_linkable_identifiers]) for x in linkable_element_sets]
            )
            if len(linkable_element_sets) > 0
            else set()
        )

        return LinkableElementSet(
            linkable_dimensions=tuple(common_linkable_dimensions),
            linkable_identifiers=tuple(common_linkable_identifiers),
            ambiguous_linkable_dimensions=tuple(common_ambiguous_linkable_dimensions),
            ambiguous_linkable_identifiers=tuple(common_ambiguous_linkable_identifiers),
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

        linkable_identifiers = tuple(
            x
            for x in self.linkable_identifiers
            if len(x.properties.intersection(with_any_of)) > 0 and len(x.properties.intersection(without_any_of)) == 0
        )

        ambiguous_linkable_dimensions = tuple(
            x
            for x in self.ambiguous_linkable_dimensions
            if len(x.properties.intersection(with_any_of)) > 0 and len(x.properties.intersection(without_any_of)) == 0
        )

        ambiguous_linkable_identifiers = tuple(
            x
            for x in self.ambiguous_linkable_identifiers
            if len(x.properties.intersection(with_any_of)) > 0 and len(x.properties.intersection(without_any_of)) == 0
        )

        return LinkableElementSet(
            linkable_dimensions=linkable_dimensions,
            linkable_identifiers=linkable_identifiers,
            ambiguous_linkable_dimensions=ambiguous_linkable_dimensions,
            ambiguous_linkable_identifiers=ambiguous_linkable_identifiers,
        )

    @property
    def as_spec_set(self) -> LinkableSpecSet:  # noqa: D
        return LinkableSpecSet(
            dimension_specs=tuple(
                DimensionSpec(
                    element_name=x.element_name,
                    identifier_links=tuple(IdentifierReference(element_name=x) for x in x.identifier_links),
                )
                for x in self.linkable_dimensions
                if not x.time_granularity
            ),
            time_dimension_specs=tuple(
                TimeDimensionSpec(
                    element_name=x.element_name,
                    identifier_links=tuple(IdentifierReference(element_name=x) for x in x.identifier_links),
                    time_granularity=x.time_granularity,
                )
                for x in self.linkable_dimensions
                if x.time_granularity
            ),
            identifier_specs=tuple(
                IdentifierSpec(
                    element_name=x.element_name,
                    identifier_links=tuple(IdentifierReference(element_name=x) for x in x.identifier_links),
                )
                for x in self.linkable_identifiers
            ),
        )


@dataclass(frozen=True)
class EntityJoinPathElement:
    """Describes joining a entity by the given identifier."""

    entity: Entity
    join_on_identifier: str


def _generate_linkable_time_dimensions(
    dimension: Dimension, identifier_links: Tuple[str, ...], with_properties: FrozenSet[LinkableElementProperties]
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
                identifier_links=identifier_links,
                time_granularity=time_granularity,
                properties=frozenset(properties),
            )
        )

    return linkable_dimensions


@dataclass(frozen=True)
class EntityJoinPath:
    """Describes a series of joins between the measure entity, and other entities by identifier.

    For example:

    (measure_source JOIN dimension_source0 ON identifier A) JOIN dimension_source1 ON identifier B

    would be represented by 2 path elements [(entity0, A), (dimension_source1, B)]
    """

    path_elements: Tuple[EntityJoinPathElement, ...]

    def create_linkable_element_set(self, with_properties: FrozenSet[LinkableElementProperties]) -> LinkableElementSet:
        """Given the current path, generate the respective linkable elements from the last entity in the path."""
        identifier_links = tuple(x.join_on_identifier for x in self.path_elements)

        assert len(self.path_elements) > 0
        entity = self.path_elements[-1].entity

        linkable_dimensions = []
        linkable_identifiers = []

        for dimension in entity.dimensions:
            dimension_type = dimension.type
            if dimension_type == DimensionType.CATEGORICAL:
                linkable_dimensions.append(
                    LinkableDimension(
                        element_name=dimension.reference.element_name,
                        identifier_links=identifier_links,
                        properties=with_properties,
                    )
                )
            elif dimension_type == DimensionType.TIME:
                linkable_dimensions.extend(
                    _generate_linkable_time_dimensions(
                        dimension=dimension,
                        identifier_links=identifier_links,
                        with_properties=with_properties,
                    )
                )
            else:
                raise RuntimeError(f"Unhandled type: {dimension_type}")

        for identifier in entity.identifiers:
            # Avoid creating "booking_id__booking_id"
            if identifier.reference.element_name != identifier_links[-1]:
                linkable_identifiers.append(
                    LinkableIdentifier(
                        element_name=identifier.reference.element_name,
                        identifier_links=identifier_links,
                        properties=with_properties.union({LinkableElementProperties.IDENTIFIER}),
                    )
                )

        return LinkableElementSet(
            linkable_dimensions=tuple(linkable_dimensions),
            linkable_identifiers=tuple(linkable_identifiers),
            ambiguous_linkable_dimensions=(),
            ambiguous_linkable_identifiers=(),
        )

    @property
    def last_entity(self) -> Entity:
        """The last entity that would be joined in this path."""
        assert len(self.path_elements) > 0
        return self.path_elements[-1].entity


class ValidLinkableSpecResolver:
    """Figures out what linkable specs are valid for a given metric.

    e.g. Can you query the metric "bookings" by "listing__country_latest"?
    """

    def __init__(
        self,
        user_configured_model: UserConfiguredModel,
        entity_semantics: EntitySemanticsAccessor,
        max_identifier_links: int,
    ) -> None:
        """Constructor.

        Args:
            user_configured_model: the model to use.
            entity_semantics: used to look up identifiers for a entity.
            max_identifier_links: the maximum number of joins to do when computing valid elements.
        """
        self._user_configured_model = user_configured_model
        # Sort entities by name for consistency in building derived objects.
        self._entities = sorted(self._user_configured_model.entities, key=lambda x: x.name)
        self._join_evaluator = EntityJoinEvaluator(entity_semantics)

        assert max_identifier_links >= 0
        self._max_identifier_links = max_identifier_links

        # Map measures / identifiers to entities that contain them.
        self._identifier_to_entity: Dict[str, List[Entity]] = defaultdict(list)
        self._measure_to_entity: Dict[str, List[Entity]] = defaultdict(list)

        for entity in self._entities:
            for identifier in entity.identifiers:
                self._identifier_to_entity[identifier.reference.element_name].append(entity)

        self._metric_to_linkable_element_sets: Dict[str, List[LinkableElementSet]] = {}

        start_time = time.time()
        for metric in self._user_configured_model.metrics:
            linkable_sets_for_measure = []
            for measure in metric.measure_references:
                linkable_sets_for_measure.append(self._get_linkable_element_set_for_measure(measure))

            self._metric_to_linkable_element_sets[metric.name] = linkable_sets_for_measure
        logger.info(f"Building the [metric -> valid linkable element] index took: {time.time() - start_time:.2f}s")

    def _get_entity_for_measure(self, measure_reference: MeasureReference) -> Entity:  # noqa: D
        entities_where_measure_was_found = []
        for entity in self._entities:
            if any([x.reference.element_name == measure_reference.element_name for x in entity.measures]):
                entities_where_measure_was_found.append(entity)

        if len(entities_where_measure_was_found) == 0:
            raise ValueError(f"No entities were found with {measure_reference} in the model")
        elif len(entities_where_measure_was_found) > 1:
            raise ValueError(
                f"Measure {measure_reference} was found in multiple entities:\n"
                f"{pformat_big_objects(entities_where_measure_was_found)}"
            )
        return entities_where_measure_was_found[0]

    def _get_local_set(self, entity: Entity) -> LinkableElementSet:
        """Gets the local elements for a given entity."""
        linkable_dimensions = []
        linkable_identifiers = []

        for dimension in entity.dimensions:
            dimension_type = dimension.type
            if dimension_type == DimensionType.CATEGORICAL:
                linkable_dimensions.append(
                    LinkableDimension(
                        element_name=dimension.reference.element_name,
                        identifier_links=(),
                        properties=frozenset({LinkableElementProperties.LOCAL}),
                    )
                )
            elif dimension_type == DimensionType.TIME:
                linkable_dimensions.extend(
                    _generate_linkable_time_dimensions(
                        dimension=dimension,
                        identifier_links=(),
                        with_properties=frozenset({LinkableElementProperties.LOCAL}),
                    )
                )
            else:
                raise RuntimeError(f"Unhandled type: {dimension_type}")

        additional_linkable_dimensions = []
        for identifier in entity.identifiers:
            linkable_identifiers.append(
                LinkableIdentifier(
                    element_name=identifier.reference.element_name,
                    identifier_links=(),
                    properties=frozenset({LinkableElementProperties.LOCAL, LinkableElementProperties.IDENTIFIER}),
                )
            )
            # If a entity has a primary identifier, we allow users to query using the dundered syntax, even though
            # there is no join involved. e.g. in the test model, the "listings_latest" entity would allow using
            # "listing__country_latest" for the "listings" metric.
            if identifier.type == IdentifierType.PRIMARY:
                for linkable_dimension in linkable_dimensions:
                    properties = {LinkableElementProperties.LOCAL, LinkableElementProperties.LOCAL_LINKED}
                    additional_linkable_dimensions.append(
                        LinkableDimension(
                            element_name=linkable_dimension.element_name,
                            identifier_links=(identifier.reference.element_name,),
                            time_granularity=linkable_dimension.time_granularity,
                            properties=frozenset(linkable_dimension.properties.union(properties)),
                        )
                    )

        return LinkableElementSet(
            linkable_dimensions=tuple(linkable_dimensions + additional_linkable_dimensions),
            linkable_identifiers=tuple(linkable_identifiers),
            ambiguous_linkable_dimensions=(),
            ambiguous_linkable_identifiers=(),
        )

    def _get_entities_with_joinable_identifier(
        self,
        left_entity_reference: EntityReference,
        identifier_reference: IdentifierReference,
    ) -> Sequence[Entity]:
        # May switch to non-cached implementation.
        entities = self._identifier_to_entity[identifier_reference.element_name]
        valid_entities = []
        for entity in entities:
            if self._join_evaluator.is_valid_entity_join(
                left_entity_reference=left_entity_reference,
                right_entity_reference=entity.reference,
                on_identifier_reference=identifier_reference,
            ):
                valid_entities.append(entity)
        return valid_entities

    def _get_linkable_element_set_for_measure(self, measure_reference: MeasureReference) -> LinkableElementSet:
        """Get the valid linkable elements for the given measure."""
        measure_entity = self._get_entity_for_measure(measure_reference)

        # Create local elements
        local_linkable_elements = self._get_local_set(measure_entity)
        join_paths = []

        # Create 1-hop elements
        for identifier in measure_entity.identifiers:
            entities = self._get_entities_with_joinable_identifier(
                left_entity_reference=measure_entity.reference,
                identifier_reference=identifier.reference,
            )
            for entity in entities:
                if entity.name == measure_entity.name:
                    continue
                join_paths.append(
                    EntityJoinPath(
                        path_elements=(
                            EntityJoinPathElement(
                                entity=entity, join_on_identifier=identifier.reference.element_name
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
        # path, extend all paths to include the next valid entity, then repeat.
        for i in range(self._max_identifier_links - 1):
            new_join_paths: List[EntityJoinPath] = []
            for join_path in join_paths:
                new_join_paths.extend(
                    self._find_next_possible_paths(measure_entity=measure_entity, current_join_path=join_path)
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
        self, measure_entity: Entity, current_join_path: EntityJoinPath
    ) -> Sequence[EntityJoinPath]:
        """Generate the set of possible paths that are 1 entity join longer that the "current_join_path"."""
        last_entity_in_path = current_join_path.last_entity
        new_join_paths = []

        for identifier in last_entity_in_path.identifiers:
            identifier_name = identifier.reference.element_name

            # Don't create cycles in the join path by joining on the same identifier.
            if identifier_name in set(x.join_on_identifier for x in current_join_path.path_elements):
                continue

            entities_that_can_be_joined = self._get_entities_with_joinable_identifier(
                left_entity_reference=last_entity_in_path.reference,
                identifier_reference=identifier.reference,
            )
            for entity in entities_that_can_be_joined:
                # Don't create cycles in the join path by repeating a entity in the path.
                if entity.name == measure_entity.name or any(
                    tuple(x.entity.name == entity.name for x in current_join_path.path_elements)
                ):
                    continue

                new_join_path = EntityJoinPath(
                    path_elements=current_join_path.path_elements
                    + (EntityJoinPathElement(entity=entity, join_on_identifier=identifier_name),)
                )
                new_join_paths.append(new_join_path)

        return new_join_paths
