from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Sequence, Set, Tuple

from metricflow.model.semantics.linkable_element import (
    ElementPathKey,
    LinkableDimension,
    LinkableElementProperty,
    LinkableElementType,
    LinkableEntity,
    LinkableMetric,
)
from metricflow.specs.specs import DimensionSpec, EntitySpec, GroupByMetricSpec, LinkableSpecSet, TimeDimensionSpec


@dataclass(frozen=True)
class LinkableElementSet:
    """Container class for storing all linkable elements for a metric.

    TODO: There are similarities with LinkableSpecSet - consider consolidation.
    """

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
        with_any_of: FrozenSet[LinkableElementProperty],
        without_any_of: FrozenSet[LinkableElementProperty] = frozenset(),
        without_all_of: FrozenSet[LinkableElementProperty] = frozenset(),
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
                if path_key.element_type is LinkableElementType.DIMENSION
            ),
            time_dimension_specs=tuple(
                TimeDimensionSpec(
                    element_name=path_key.element_name,
                    entity_links=path_key.entity_links,
                    time_granularity=path_key.time_granularity,
                    date_part=path_key.date_part,
                )
                for path_key in self.path_key_to_linkable_dimensions.keys()
                if path_key.element_type is LinkableElementType.TIME_DIMENSION and path_key.time_granularity
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
