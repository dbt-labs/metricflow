from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Tuple, Sequence, Dict, List, Optional, FrozenSet

from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.elements.dimension import DimensionType, Dimension
from metricflow.model.objects.elements.identifier import IdentifierType
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.object_utils import pformat_big_objects, flatten_nested_sequence
from metricflow.specs import (
    MeasureReference,
    LinklessIdentifierSpec,
    DEFAULT_TIME_GRANULARITY,
    LinkableSpecSet,
    DimensionSpec,
    TimeDimensionSpec,
    IdentifierSpec,
    MetricSpec,
    TimeDimensionReference,
)
from metricflow.time.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


class LinkableElementProperties(Enum):
    """The properties associated with a valid linkable element.

    Local means an element that is defined within the same data source as the measure. This definition is used
    throughout the related classes.
    """

    # A primary time dimension that is accessed by a primary identifier within the data source. e.g. in the test data
    # source "listings_latest", an example is "listing__ds"
    LOCAL_LINKED_PRIMARY_TIME = "local_linked_primary_time"
    # A local element as per above definition.
    LOCAL = "local"
    # An element that was joined to the measure data source by an identifier.
    JOINED = "joined"
    # An element that was joined to the measure data source by joining multiple data sources.
    MULTI_HOP = "multi_hop"
    # A time dimension that is a version of a time dimension in a data source, but at a different granularity.
    DERIVED_TIME_GRANULARITY = "derived_time_granularity"
    # After an intersection operation.
    INTERSECTED = "intersected"

    @staticmethod
    def all_properties() -> FrozenSet[LinkableElementProperties]:  # noqa: D
        return frozenset(
            {
                LinkableElementProperties.LOCAL_LINKED_PRIMARY_TIME,
                LinkableElementProperties.LOCAL,
                LinkableElementProperties.JOINED,
                LinkableElementProperties.MULTI_HOP,
                LinkableElementProperties.DERIVED_TIME_GRANULARITY,
            }
        )


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

    # Ambiguous elements are ones where there are multiple join paths through different data sources that can be taken
    # to get the element. This currently represents an error when defining data sources.
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
                    identifier_links=tuple(LinklessIdentifierSpec.from_element_name(x) for x in x.identifier_links),
                )
                for x in self.linkable_dimensions
                if not x.time_granularity
            ),
            time_dimension_specs=tuple(
                TimeDimensionSpec(
                    element_name=x.element_name,
                    identifier_links=tuple(LinklessIdentifierSpec.from_element_name(x) for x in x.identifier_links),
                    time_granularity=x.time_granularity,
                )
                for x in self.linkable_dimensions
                if x.time_granularity
            ),
            identifier_specs=tuple(
                IdentifierSpec(
                    element_name=x.element_name,
                    identifier_links=tuple(LinklessIdentifierSpec.from_element_name(x) for x in x.identifier_links),
                )
                for x in self.linkable_identifiers
            ),
        )


@dataclass(frozen=True)
class DataSourceJoinPathElement:
    """Describes joining a data source by the given identifier."""

    data_source: DataSource
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
                element_name=dimension.name.element_name,
                identifier_links=identifier_links,
                time_granularity=time_granularity,
                properties=frozenset(properties),
            )
        )

    return linkable_dimensions


@dataclass(frozen=True)
class DataSourceJoinPath:
    """Describes a series of joins between the measure data source, and other data sources by identifier.

    For example:

    (measure_source JOIN dimension_source0 ON identifier A) JOIN dimension_source1 ON identifier B

    would be represented by 2 path elements [(data_source0, A), (dimension_source1, B)]
    """

    path_elements: Tuple[DataSourceJoinPathElement, ...]

    def create_linkable_element_set(self, with_properties: FrozenSet[LinkableElementProperties]) -> LinkableElementSet:
        """Given the current path, generate the respective linkable elements from the last data source in the path."""
        identifier_links = tuple(x.join_on_identifier for x in self.path_elements)

        assert len(self.path_elements) > 0
        data_source = self.path_elements[-1].data_source

        linkable_dimensions = []
        linkable_identifiers = []

        for dimension in data_source.dimensions:
            dimension_type = dimension.type
            if dimension_type == DimensionType.CATEGORICAL:
                linkable_dimensions.append(
                    LinkableDimension(
                        element_name=dimension.name.element_name,
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

        for identifier in data_source.identifiers:
            # Avoid creating "booking_id__booking_id"
            if identifier.name.element_name != identifier_links[-1]:
                linkable_identifiers.append(
                    LinkableIdentifier(
                        element_name=identifier.name.element_name,
                        identifier_links=identifier_links,
                        properties=with_properties,
                    )
                )

        return LinkableElementSet(
            linkable_dimensions=tuple(linkable_dimensions),
            linkable_identifiers=tuple(linkable_identifiers),
            ambiguous_linkable_dimensions=(),
            ambiguous_linkable_identifiers=(),
        )

    @property
    def last_data_source(self) -> DataSource:
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
        primary_time_dimension_reference: TimeDimensionReference,
        max_identifier_links: int,
    ) -> None:
        """Constructor.

        Args:
            user_configured_model: the model to use.
            primary_time_dimension_reference: the primary time dimension in the model.
            max_identifier_links: the maximum number of joins to do when computing valid elements.
        """
        self._user_configured_model = user_configured_model
        self._primary_time_dimension_reference = primary_time_dimension_reference
        self._data_sources = self._user_configured_model.data_sources

        assert max_identifier_links >= 0
        self._max_identifier_links = max_identifier_links

        # Map measures / identifiers to data sources that contain them.
        self._identifier_to_data_source: Dict[str, List[DataSource]] = defaultdict(list)
        self._measure_to_data_source: Dict[str, List[DataSource]] = defaultdict(list)

        for data_source in self._data_sources:
            for identifier in data_source.identifiers:
                if identifier.type in (IdentifierType.PRIMARY, IdentifierType.UNIQUE):
                    self._identifier_to_data_source[identifier.name.element_name].append(data_source)

        self._metric_to_linkable_element_sets: Dict[str, List[LinkableElementSet]] = {}

        start_time = time.time()
        for metric in self._user_configured_model.metrics:
            linkable_sets_for_measure = []
            for measure in metric.measure_names:
                linkable_sets_for_measure.append(self._get_linkable_element_set_for_measure(measure))

            self._metric_to_linkable_element_sets[metric.name] = linkable_sets_for_measure
        logger.info(f"Building the [metric -> valid linkable element] index took: {time.time() - start_time:.2f}s")

    def _get_data_source_for_measure(self, measure_reference: MeasureReference) -> DataSource:  # noqa: D
        data_sources_where_measure_was_found = []
        for data_source in self._data_sources:
            if any([x.name.element_name == measure_reference.element_name for x in data_source.measures]):
                data_sources_where_measure_was_found.append(data_source)

        if len(data_sources_where_measure_was_found) == 0:
            raise ValueError(f"No data sources were found with {measure_reference} in the model")
        elif len(data_sources_where_measure_was_found) > 1:
            raise ValueError(
                f"Measure {measure_reference} was found in multiple data sources:\n"
                f"{pformat_big_objects(data_sources_where_measure_was_found)}"
            )
        return data_sources_where_measure_was_found[0]

    def _get_local_set(self, data_source: DataSource) -> LinkableElementSet:
        """Gets the local elements for a given data source."""
        linkable_dimensions = []
        linkable_identifiers = []

        for dimension in data_source.dimensions:
            dimension_type = dimension.type
            if dimension_type == DimensionType.CATEGORICAL:
                linkable_dimensions.append(
                    LinkableDimension(
                        element_name=dimension.name.element_name,
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
        for identifier in data_source.identifiers:
            linkable_identifiers.append(
                LinkableIdentifier(
                    element_name=identifier.name.element_name,
                    identifier_links=(),
                    properties=frozenset({LinkableElementProperties.LOCAL}),
                )
            )
            # If a data source has a primary identifier, we allow users to query using the dundered syntax, even though
            # there is no join involved. e.g. in the test model, the "listings_latest" data source would allow using
            # "listing__country_latest" for the "listings" metric.
            if identifier.type == IdentifierType.PRIMARY:
                for linkable_dimension in linkable_dimensions:
                    properties = {LinkableElementProperties.LOCAL}

                    if linkable_dimension.element_name == self._primary_time_dimension_reference.element_name:
                        properties.add(LinkableElementProperties.LOCAL_LINKED_PRIMARY_TIME)

                    additional_linkable_dimensions.append(
                        LinkableDimension(
                            element_name=linkable_dimension.element_name,
                            identifier_links=(identifier.name.element_name,),
                            time_granularity=linkable_dimension.time_granularity,
                            properties=linkable_dimension.properties,
                        )
                    )

        return LinkableElementSet(
            linkable_dimensions=tuple(linkable_dimensions + additional_linkable_dimensions),
            linkable_identifiers=tuple(linkable_identifiers),
            ambiguous_linkable_dimensions=(),
            ambiguous_linkable_identifiers=(),
        )

    def _get_data_sources_with_joinable_identifier(self, identifier_name: str) -> Sequence[DataSource]:
        # May switch to non-cached implementation.
        data_sources = self._identifier_to_data_source[identifier_name]
        return data_sources

    def _get_linkable_element_set_for_measure(self, measure_reference: MeasureReference) -> LinkableElementSet:
        """Get the valid linkable elements for the given measure."""
        measure_data_source = self._get_data_source_for_measure(measure_reference)

        # Create local elements
        local_linkable_elements = self._get_local_set(measure_data_source)
        join_paths = []

        # Create 1-hop elements
        for identifier in measure_data_source.identifiers:
            data_sources = self._get_data_sources_with_joinable_identifier(identifier.name.element_name)
            for data_source in data_sources:
                if data_source.name == measure_data_source.name:
                    continue
                join_paths.append(
                    DataSourceJoinPath(
                        path_elements=(
                            DataSourceJoinPathElement(
                                data_source=data_source, join_on_identifier=identifier.name.element_name
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
        for i in range(self._max_identifier_links - 1):
            new_join_paths: List[DataSourceJoinPath] = []
            for join_path in join_paths:
                new_join_paths.extend(
                    self._find_next_possible_paths(measure_data_source=measure_data_source, current_join_path=join_path)
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
        metric_specs: Sequence[MetricSpec],
        with_any_of: FrozenSet[LinkableElementProperties],
        without_any_of: FrozenSet[LinkableElementProperties],
    ) -> LinkableElementSet:
        """Gets the valid linkable elements that are common to all requested metrics."""
        linkable_element_sets = []
        for metric_spec in metric_specs:
            element_sets = self._metric_to_linkable_element_sets.get(metric_spec.element_name)
            if not element_sets:
                raise ValueError(f"Unknown metric: {metric_spec} in element set")
            metric_result = LinkableElementSet.intersection(
                [x.filter(with_any_of=with_any_of, without_any_of=without_any_of) for x in element_sets]
            )
            linkable_element_sets.append(metric_result)

        return LinkableElementSet.intersection(linkable_element_sets)

    def _find_next_possible_paths(
        self, measure_data_source: DataSource, current_join_path: DataSourceJoinPath
    ) -> Sequence[DataSourceJoinPath]:
        """Generate the set of possible paths that are 1 data source join longer that the "current_join_path"."""
        data_source = current_join_path.last_data_source
        new_join_paths = []

        for identifier in data_source.identifiers:
            identifier_name = identifier.name.element_name

            if identifier_name in set(x.join_on_identifier for x in current_join_path.path_elements):
                continue

            data_sources_that_can_be_joined = self._get_data_sources_with_joinable_identifier(identifier_name)
            for data_source in data_sources_that_can_be_joined:
                # Don't create cycles in the join path by not repeating a data source in the path.
                if data_source.name == measure_data_source.name or any(
                    tuple(x.data_source.name == data_source.name for x in current_join_path.path_elements)
                ):
                    continue

                new_join_path = DataSourceJoinPath(
                    path_elements=current_join_path.path_elements
                    + (DataSourceJoinPathElement(data_source=data_source, join_on_identifier=identifier_name),)
                )
                new_join_paths.append(new_join_path)

        return new_join_paths
