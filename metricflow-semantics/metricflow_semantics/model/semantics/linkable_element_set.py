from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from functools import cached_property
from typing import Dict, List, Sequence, Set, Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.references import SemanticModelReference
from typing_extensions import override

from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.semantic_model_derivation import SemanticModelDerivation
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_element import (
    ElementPathKey,
    LinkableDimension,
    LinkableElement,
    LinkableElementType,
    LinkableEntity,
    LinkableMetric,
)
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
from metricflow_semantics.specs.instance_spec import InstanceSpec, LinkableInstanceSpec
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LinkableElementSet(SemanticModelDerivation):
    """Container class for storing all linkable elements for a metric.

    TODO: There are similarities with LinkableSpecSet - consider consolidation.
    """

    path_key_to_linkable_dimensions: Dict[ElementPathKey, Tuple[LinkableDimension, ...]] = field(default_factory=dict)
    path_key_to_linkable_entities: Dict[ElementPathKey, Tuple[LinkableEntity, ...]] = field(default_factory=dict)
    path_key_to_linkable_metrics: Dict[ElementPathKey, Tuple[LinkableMetric, ...]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Basic validation for ensuring consistency between path key type and value type."""
        mismatched_dimensions = tuple(
            path_key
            for path_key in self.path_key_to_linkable_dimensions.keys()
            if not path_key.element_type.is_dimension_type
        )
        mismatched_entities = tuple(
            path_key
            for path_key in self.path_key_to_linkable_entities
            if path_key.element_type is not LinkableElementType.ENTITY
        )
        mismatched_metrics = tuple(
            path_key
            for path_key in self.path_key_to_linkable_metrics
            if path_key.element_type is not LinkableElementType.METRIC
        )

        mismatched_elements = {
            "dimensions": mismatched_dimensions,
            "entities": mismatched_entities,
            "metrics": mismatched_metrics,
        }

        assert all(len(mismatches) == 0 for mismatches in mismatched_elements.values()), (
            f"Found one or more elements where the element type defined in the path key does not match the value "
            f"type! Mismatched elements: {mismatched_elements}"
        )

        # There shouldn't be a path key without any concrete items. Can be an issue as specs contained in this set are
        # generated from the path keys.
        for key, value in (
            tuple(self.path_key_to_linkable_dimensions.items())
            + tuple(self.path_key_to_linkable_entities.items())
            + tuple(self.path_key_to_linkable_metrics.items())
        ):
            assert len(value) > 0, f"{key} is empty"

        # There shouldn't be any duplicate specs.
        specs = self.specs
        deduped_specs = set(specs)
        assert len(deduped_specs) == len(specs)
        assert len(deduped_specs) == (
            len(self.path_key_to_linkable_dimensions)
            + len(self.path_key_to_linkable_entities)
            + len(self.path_key_to_linkable_metrics)
        )

        # Check time dimensions have the grain set.
        for path_key, linkable_dimensions in self.path_key_to_linkable_dimensions.items():
            if path_key.element_type is LinkableElementType.TIME_DIMENSION:
                for linkable_dimension in linkable_dimensions:
                    assert (
                        linkable_dimension.time_granularity is not None
                    ), f"{path_key} has a dimension without the time granularity set: {linkable_dimension}"

    @staticmethod
    def merge_by_path_key(linkable_element_sets: Sequence[LinkableElementSet]) -> LinkableElementSet:
        """Combine multiple sets together by the path key.

        If there are elements with the same join key and different element(s) in the tuple of values,
        those elements will be categorized as ambiguous.
        Note this does not deduplicate values, so there may be unambiguous merged sets that appear to have
        multiple values if all one does is a simple length check.
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

        This will return the intersection of all path keys defined in the sets, but the union of elements associated
        with each path key. In other words, it filters out path keys (i.e., linkable specs) that are not referenced
        in every set in the input sequence, but it preserves all of the various potentially ambiguous LinkableElement
        instances associated with the path keys that remain.

        This is useful to figure out the common dimensions that are possible to query with multiple metrics. You would
        find the LinkableSpecSet for each metric in the query, then do an intersection of the sets.
        """
        if len(linkable_element_sets) == 0:
            return LinkableElementSet()
        elif len(linkable_element_sets) == 1:
            return linkable_element_sets[0]

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
                            if linkable_dimension.defined_in_semantic_model
                            else ""
                        ),
                    )
                )
                for path_key, dimensions in join_path_to_linkable_dimensions.items()
            },
            path_key_to_linkable_entities={
                path_key: tuple(
                    sorted(
                        entities,
                        key=lambda linkable_entity: linkable_entity.defined_in_semantic_model.semantic_model_name,
                    )
                )
                for path_key, entities in join_path_to_linkable_entities.items()
            },
            path_key_to_linkable_metrics={
                path_key: tuple(sorted(metrics, key=lambda linkable_metric: linkable_metric.element_name))
                for path_key, metrics in join_path_to_linkable_metrics.items()
            },
        )

    def linkable_elements_for_path_key(self, path_key: ElementPathKey) -> Sequence[LinkableElement]:
        """Returns the linkable elements associated with the given path key in this set.

        If the path key does not exist in the set, this silently returns an empty Sequence.
        """
        if path_key in self.path_key_to_linkable_dimensions:
            return self.path_key_to_linkable_dimensions[path_key]
        elif path_key in self.path_key_to_linkable_entities:
            return self.path_key_to_linkable_entities[path_key]
        elif path_key in self.path_key_to_linkable_metrics:
            return self.path_key_to_linkable_metrics[path_key]
        else:
            return tuple()

    def filter(
        self,
        element_filter: LinkableElementFilter,
    ) -> LinkableElementSet:
        """Filter elements in the set.

        First, only elements with at least one property in the "with_any_of" set are retained. Then, any elements with
        a property in "without_any_of" set are removed. Lastly, any elements with all properties in without_all_of
        are removed.
        """
        element_names = element_filter.element_names
        with_any_of = element_filter.with_any_of
        without_any_of = element_filter.without_any_of
        without_all_of = element_filter.without_all_of

        key_to_linkable_dimensions: Dict[ElementPathKey, Tuple[LinkableDimension, ...]] = {}
        key_to_linkable_entities: Dict[ElementPathKey, Tuple[LinkableEntity, ...]] = {}
        key_to_linkable_metrics: Dict[ElementPathKey, Tuple[LinkableMetric, ...]] = {}

        for path_key, linkable_dimensions in self.path_key_to_linkable_dimensions.items():
            if element_names is not None and path_key.element_name not in element_names:
                continue

            filtered_linkable_dimensions = tuple(
                linkable_dimension
                for linkable_dimension in linkable_dimensions
                if len(linkable_dimension.property_set.intersection(with_any_of)) > 0
                and len(linkable_dimension.property_set.intersection(without_any_of)) == 0
                and (
                    len(without_all_of) == 0
                    or linkable_dimension.property_set.intersection(without_all_of) != without_all_of
                )
            )
            if len(filtered_linkable_dimensions) > 0:
                key_to_linkable_dimensions[path_key] = filtered_linkable_dimensions

        for path_key, linkable_entities in self.path_key_to_linkable_entities.items():
            if element_names is not None and path_key.element_name not in element_names:
                continue

            filtered_linkable_entities = tuple(
                linkable_entity
                for linkable_entity in linkable_entities
                if len(linkable_entity.property_set.intersection(with_any_of)) > 0
                and len(linkable_entity.property_set.intersection(without_any_of)) == 0
                and (
                    len(without_all_of) == 0
                    or linkable_entity.property_set.intersection(without_all_of) != without_all_of
                )
            )
            if len(filtered_linkable_entities) > 0:
                key_to_linkable_entities[path_key] = filtered_linkable_entities

        for path_key, linkable_metrics in self.path_key_to_linkable_metrics.items():
            if element_names is not None and path_key.element_name not in element_names:
                continue

            filtered_linkable_metrics = tuple(
                linkable_metric
                for linkable_metric in linkable_metrics
                if len(linkable_metric.property_set.intersection(with_any_of)) > 0
                and len(linkable_metric.property_set.intersection(without_any_of)) == 0
                and (
                    len(without_all_of) == 0
                    or linkable_metric.property_set.intersection(without_all_of) != without_all_of
                )
            )
            if len(filtered_linkable_metrics) > 0:
                key_to_linkable_metrics[path_key] = filtered_linkable_metrics

        return LinkableElementSet(
            path_key_to_linkable_dimensions=key_to_linkable_dimensions,
            path_key_to_linkable_entities=key_to_linkable_entities,
            path_key_to_linkable_metrics=key_to_linkable_metrics,
        )

    @cached_property
    def only_unique_path_keys(self) -> LinkableElementSet:
        """Returns a set that only includes path keys that map to a single distinct element."""
        return LinkableElementSet(
            path_key_to_linkable_dimensions={
                path_key: tuple(set(linkable_dimensions))
                for path_key, linkable_dimensions in self.path_key_to_linkable_dimensions.items()
                if len(set(linkable_dimensions)) <= 1
            },
            path_key_to_linkable_entities={
                path_key: tuple(set(linkable_entities))
                for path_key, linkable_entities in self.path_key_to_linkable_entities.items()
                if len(set(linkable_entities)) <= 1
            },
            path_key_to_linkable_metrics={
                path_key: tuple(set(linkable_metrics))
                for path_key, linkable_metrics in self.path_key_to_linkable_metrics.items()
                if len(set(linkable_metrics)) <= 1
            },
        )

    @cached_property
    @override
    def derived_from_semantic_models(self) -> Sequence[SemanticModelReference]:
        semantic_model_references: Set[SemanticModelReference] = set()
        for linkable_dimensions in self.path_key_to_linkable_dimensions.values():
            for linkable_dimension in linkable_dimensions:
                semantic_model_references.update(linkable_dimension.derived_from_semantic_models)
        for linkable_entities in self.path_key_to_linkable_entities.values():
            for linkable_entity in linkable_entities:
                semantic_model_references.update(linkable_entity.derived_from_semantic_models)
        for linkable_metrics in self.path_key_to_linkable_metrics.values():
            for linkable_metric in linkable_metrics:
                semantic_model_references.update(linkable_metric.derived_from_semantic_models)

        return sorted(semantic_model_references, key=lambda reference: reference.semantic_model_name)

    @cached_property
    def spec_count(self) -> int:
        """If this is mapped to spec objects, the number of specs that would be produced."""
        return (
            len(self.path_key_to_linkable_dimensions.keys())
            + len(self.path_key_to_linkable_entities.keys())
            + len(self.path_key_to_linkable_metrics.keys())
        )

    @cached_property
    def specs(self) -> Sequence[LinkableInstanceSpec]:
        """Converts the items in a `LinkableElementSet` to their corresponding spec objects."""
        specs: List[LinkableInstanceSpec] = []

        for path_key in (
            tuple(self.path_key_to_linkable_dimensions.keys())
            + tuple(self.path_key_to_linkable_entities.keys())
            + tuple(self.path_key_to_linkable_metrics.keys())
        ):
            specs.append(LinkableElementSet._path_key_to_spec(path_key))

        return specs

    @staticmethod
    def _path_key_to_spec(path_key: ElementPathKey) -> LinkableInstanceSpec:
        """Helper method to convert ElementPathKey instances to LinkableInstanceSpecs.

        This is currently used in the context of switching between ElementPathKeys and LinkableInstanceSpecs
        within a LinkableElementSet, so we leave it here for now.
        """
        if path_key.element_type is LinkableElementType.DIMENSION:
            return DimensionSpec(
                element_name=path_key.element_name,
                entity_links=path_key.entity_links,
            )
        elif path_key.element_type is LinkableElementType.TIME_DIMENSION:
            assert path_key.time_granularity is not None
            return TimeDimensionSpec(
                element_name=path_key.element_name,
                entity_links=path_key.entity_links,
                time_granularity=path_key.time_granularity,
                date_part=path_key.date_part,
            )
        elif path_key.element_type is LinkableElementType.ENTITY:
            return EntitySpec(
                element_name=path_key.element_name,
                entity_links=path_key.entity_links,
            )
        elif path_key.element_type is LinkableElementType.METRIC:
            return GroupByMetricSpec(
                element_name=path_key.element_name,
                entity_links=path_key.entity_links,
                metric_subquery_entity_links=path_key.metric_subquery_entity_links,
            )
        else:
            assert_values_exhausted(path_key.element_type)

    def filter_by_spec_patterns(self, spec_patterns: Sequence[SpecPattern]) -> LinkableElementSet:
        """Filter the elements in the set by the given spec patters.

        Returns a new set consisting of the elements in the `LinkableElementSet` that have a corresponding spec that
        match all the given spec patterns.
        """
        start_time = time.time()

        # Spec patterns need all specs to match properly e.g. `MinimumTimeGrainPattern`.
        matching_specs: Sequence[InstanceSpec] = self.specs

        for spec_pattern in spec_patterns:
            matching_specs = spec_pattern.match(matching_specs)
        specs_to_include = set(matching_specs)

        path_key_to_linkable_dimensions: Dict[ElementPathKey, Tuple[LinkableDimension, ...]] = {}
        path_key_to_linkable_entities: Dict[ElementPathKey, Tuple[LinkableEntity, ...]] = {}
        path_key_to_linkable_metrics: Dict[ElementPathKey, Tuple[LinkableMetric, ...]] = {}

        for path_key, linkable_dimensions in self.path_key_to_linkable_dimensions.items():
            if LinkableElementSet._path_key_to_spec(path_key) in specs_to_include:
                path_key_to_linkable_dimensions[path_key] = linkable_dimensions

        for path_key, linkable_entities in self.path_key_to_linkable_entities.items():
            if LinkableElementSet._path_key_to_spec(path_key) in specs_to_include:
                path_key_to_linkable_entities[path_key] = linkable_entities

        for path_key, linkable_metrics in self.path_key_to_linkable_metrics.items():
            if LinkableElementSet._path_key_to_spec(path_key) in specs_to_include:
                path_key_to_linkable_metrics[path_key] = linkable_metrics

        filtered_elements = LinkableElementSet(
            path_key_to_linkable_dimensions=path_key_to_linkable_dimensions,
            path_key_to_linkable_entities=path_key_to_linkable_entities,
            path_key_to_linkable_metrics=path_key_to_linkable_metrics,
        )
        logger.debug(LazyFormat(lambda: f"Filtering valid linkable elements took: {time.time() - start_time:.2f}s"))
        return filtered_elements

    def filter_by_left_semantic_model(
        self, left_semantic_model_reference: SemanticModelReference
    ) -> LinkableElementSet:
        """Return a `LinkableElementSet` with only elements that have the given left semantic model in the join path."""
        path_key_to_linkable_dimensions: Dict[ElementPathKey, Tuple[LinkableDimension, ...]] = {}
        path_key_to_linkable_entities: Dict[ElementPathKey, Tuple[LinkableEntity, ...]] = {}
        path_key_to_linkable_metrics: Dict[ElementPathKey, Tuple[LinkableMetric, ...]] = {}

        for path_key, linkable_dimensions in self.path_key_to_linkable_dimensions.items():
            path_key_to_linkable_dimensions[path_key] = tuple(
                linkable_dimension
                for linkable_dimension in linkable_dimensions
                if linkable_dimension.join_path.left_semantic_model_reference == left_semantic_model_reference
            )

        for path_key, linkable_entities in self.path_key_to_linkable_entities.items():
            path_key_to_linkable_entities[path_key] = tuple(
                linkable_entity
                for linkable_entity in linkable_entities
                if linkable_entity.join_path.left_semantic_model_reference == left_semantic_model_reference
            )

        for path_key, linkable_metrics in self.path_key_to_linkable_metrics.items():
            path_key_to_linkable_metrics[path_key] = tuple(
                linkable_metric
                for linkable_metric in linkable_metrics
                if linkable_metric.join_path.semantic_model_join_path.left_semantic_model_reference
                == left_semantic_model_reference
            )

        return LinkableElementSet(
            path_key_to_linkable_dimensions=path_key_to_linkable_dimensions,
            path_key_to_linkable_entities=path_key_to_linkable_entities,
            path_key_to_linkable_metrics=path_key_to_linkable_metrics,
        )
