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

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.ordered_set import MutableOrderedSet
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_element import (
    ElementPathKey,
    LinkableDimension,
    LinkableElement,
    LinkableElementType,
    LinkableEntity,
    LinkableMetric,
)
from metricflow_semantics.model.semantics.linkable_element_set_base import AnnotatedSpec, BaseLinkableElementSet
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
from metricflow_semantics.specs.instance_spec import InstanceSpec, LinkableInstanceSpec
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LinkableElementSet(BaseLinkableElementSet):
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

    @staticmethod
    def _path_keys_are_semantically_equivalent(key1: ElementPathKey, key2: ElementPathKey) -> bool:
        """Check if two path keys are semantically equivalent.

        This handles the case where a PRIMARY entity path (shorter entity_links) can be equivalent
        to a FOREIGN entity path when they refer to the same logical dimension.

        For example:
        - businessunit__businessunit_name (from PRIMARY job entity) has entity_links=(businessunit,)
        - job__businessunit__businessunit_name (from FOREIGN job entity) has entity_links=(job, businessunit)

        These are semantically equivalent when:
        1. The element names match
        2. The element types match
        3. Time granularity and date parts match (for time dimensions)
        4. One path has exactly one more entity link than the other (PRIMARY vs FOREIGN relationship)
        5. The shorter path's entity_links are a suffix of the longer path's entity_links
        """
        # Quick exact match check
        if key1 == key2:
            return True

        # Must have same element name and type
        if key1.element_name != key2.element_name or key1.element_type != key2.element_type:
            return False

        # Time-specific attributes must match
        if key1.time_granularity != key2.time_granularity or key1.date_part != key2.date_part:
            return False

        # Check for PRIMARY/FOREIGN entity path equivalence
        # This only applies when one path has exactly one more entity link than the other

        len1 = len(key1.entity_links)
        len2 = len(key2.entity_links)

        # Must have exactly one difference in length for PRIMARY/FOREIGN equivalence
        if abs(len1 - len2) != 1:
            return False

        # Check if the shorter path is a suffix of the longer path
        if len1 < len2:
            # key1 is shorter, check if it's a suffix of key2
            return key2.entity_links[-len1:] == key1.entity_links
        else:
            # key2 is shorter, check if it's a suffix of key1
            return key1.entity_links[-len2:] == key2.entity_links

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
        """Perform an intersection across all LinkableElementSets by path key.

        Elements are considered to be the same if they have the same path key, or if their path keys are
        semantically equivalent (e.g., PRIMARY vs FOREIGN entity paths to the same dimension).

        When a path key exists in all sets, the union of all elements with that path key is returned.
        """
        if len(linkable_element_sets) == 0:
            return LinkableElementSet(
                path_key_to_linkable_dimensions={},
                path_key_to_linkable_entities={},
                path_key_to_linkable_metrics={},
            )
        elif len(linkable_element_sets) == 1:
            return linkable_element_sets[0]

        # Build equivalence groups across all sets
        # Map from a representative path key to all elements from all sets
        dimension_equivalence_groups: Dict[ElementPathKey, List[LinkableDimension]] = {}
        entity_equivalence_groups: Dict[ElementPathKey, List[LinkableEntity]] = {}
        metric_equivalence_groups: Dict[ElementPathKey, List[LinkableMetric]] = {}

        # Track which sets contributed to each equivalence group
        dimension_set_coverage: Dict[ElementPathKey, Set[int]] = {}
        entity_set_coverage: Dict[ElementPathKey, Set[int]] = {}
        metric_set_coverage: Dict[ElementPathKey, Set[int]] = {}

        # Process each set and group semantically equivalent elements
        for set_idx, linkable_element_set in enumerate(linkable_element_sets):
            # Process dimensions
            for path_key, dimensions in linkable_element_set.path_key_to_linkable_dimensions.items():
                # Find if this path key belongs to an existing equivalence group
                found_group = False
                for group_key in list(dimension_equivalence_groups.keys()):
                    if path_key == group_key or LinkableElementSet._path_keys_are_semantically_equivalent(
                        path_key, group_key
                    ):
                        dimension_equivalence_groups[group_key].extend(dimensions)
                        dimension_set_coverage[group_key].add(set_idx)
                        found_group = True
                        break

                if not found_group:
                    # Create a new equivalence group
                    dimension_equivalence_groups[path_key] = list(dimensions)
                    dimension_set_coverage[path_key] = {set_idx}

            # Process entities (similar logic)
            for path_key, entities in linkable_element_set.path_key_to_linkable_entities.items():
                found_group = False
                for group_key in list(entity_equivalence_groups.keys()):
                    if path_key == group_key or LinkableElementSet._path_keys_are_semantically_equivalent(
                        path_key, group_key
                    ):
                        entity_equivalence_groups[group_key].extend(entities)
                        entity_set_coverage[group_key].add(set_idx)
                        found_group = True
                        break

                if not found_group:
                    entity_equivalence_groups[path_key] = list(entities)
                    entity_set_coverage[path_key] = {set_idx}

            # Process metrics (similar logic)
            for path_key, metrics in linkable_element_set.path_key_to_linkable_metrics.items():
                found_group = False
                for group_key in list(metric_equivalence_groups.keys()):
                    if path_key == group_key or LinkableElementSet._path_keys_are_semantically_equivalent(
                        path_key, group_key
                    ):
                        metric_equivalence_groups[group_key].extend(metrics)
                        metric_set_coverage[group_key].add(set_idx)
                        found_group = True
                        break

                if not found_group:
                    metric_equivalence_groups[path_key] = list(metrics)
                    metric_set_coverage[path_key] = {set_idx}

        # Now find groups that appear in all sets
        num_sets = len(linkable_element_sets)
        final_dimensions: Dict[ElementPathKey, Set[LinkableDimension]] = {}
        final_entities: Dict[ElementPathKey, Set[LinkableEntity]] = {}
        final_metrics: Dict[ElementPathKey, Set[LinkableMetric]] = {}

        # Process dimension groups
        for group_key, dimension_elements in dimension_equivalence_groups.items():
            if len(dimension_set_coverage[group_key]) == num_sets:
                # This group appears in all sets - include all unique elements
                # If there's semantic equivalence involved, choose the representative path key
                representative_key = group_key
                # Find if any element in the group has more entity links (FOREIGN path)
                for dim_element in dimension_elements:
                    if len(dim_element.path_key.entity_links) > len(representative_key.entity_links):
                        representative_key = dim_element.path_key
                        break

                # Add all elements from this group, but use the representative key
                if representative_key not in final_dimensions:
                    final_dimensions[representative_key] = set()
                final_dimensions[representative_key].update(dimension_elements)

        # Process entity groups (similar logic)
        for group_key, entity_elements in entity_equivalence_groups.items():
            if len(entity_set_coverage[group_key]) == num_sets:
                representative_key = group_key
                for entity_element in entity_elements:
                    if len(entity_element.path_key.entity_links) > len(representative_key.entity_links):
                        representative_key = entity_element.path_key
                        break

                if representative_key not in final_entities:
                    final_entities[representative_key] = set()
                final_entities[representative_key].update(entity_elements)

        # Process metric groups (similar logic)
        for group_key, metric_elements in metric_equivalence_groups.items():
            if len(metric_set_coverage[group_key]) == num_sets:
                representative_key = group_key
                for metric_element in metric_elements:
                    if len(metric_element.path_key.entity_links) > len(representative_key.entity_links):
                        representative_key = metric_element.path_key
                        break

                if representative_key not in final_metrics:
                    final_metrics[representative_key] = set()
                final_metrics[representative_key].update(metric_elements)

        # Convert sets to sorted tuples for consistency
        return LinkableElementSet(
            path_key_to_linkable_dimensions={
                k: tuple(
                    sorted(
                        v,
                        key=lambda d: (
                            d.semantic_model_origin.semantic_model_name
                            if d.defined_in_semantic_model
                            else d.join_path.left_semantic_model_reference.semantic_model_name
                        ),
                    )
                )
                for k, v in final_dimensions.items()
            },
            path_key_to_linkable_entities={
                k: tuple(sorted(v, key=lambda e: e.defined_in_semantic_model.semantic_model_name))
                for k, v in final_entities.items()
            },
            path_key_to_linkable_metrics={
                k: tuple(sorted(v, key=lambda m: m.element_name)) for k, v in final_metrics.items()
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

    @override
    @cached_property
    def annotated_specs(self) -> Sequence[AnnotatedSpec]:
        path_key_to_linkable_elements: dict[ElementPathKey, AnyLengthTuple[LinkableElement]] = {
            **self.path_key_to_linkable_dimensions,
            **self.path_key_to_linkable_entities,
            **self.path_key_to_linkable_metrics,
        }

        annotated_specs = []

        for path_key, linkable_elements in path_key_to_linkable_elements.items():
            origin_model_ids: MutableOrderedSet[SemanticModelId] = MutableOrderedSet()
            properties: MutableOrderedSet[LinkableElementProperty] = MutableOrderedSet()
            derived_from_semantic_models: MutableOrderedSet[SemanticModelReference] = MutableOrderedSet()
            for linkable_element in linkable_elements:
                origin_model_ids.add(
                    SemanticModelId.get_instance(linkable_element.semantic_model_origin.semantic_model_name)
                )
                properties.update(linkable_element.properties)
                derived_from_semantic_models.update(linkable_element.derived_from_semantic_models)

            annotated_specs.append(
                AnnotatedSpec.create(
                    element_type=path_key.element_type,
                    element_name=path_key.element_name,
                    entity_links=path_key.entity_links,
                    time_grain=path_key.time_granularity,
                    date_part=path_key.date_part,
                    metric_subquery_entity_links=path_key.metric_subquery_entity_links,
                    properties=properties,
                    origin_model_ids=origin_model_ids,
                    derived_from_semantic_models=derived_from_semantic_models,
                )
            )
        return tuple(annotated_specs)

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
        """Filter the elements in the set by the given spec patterns.

        Returns a new set consisting of the elements in the `LinkableElementSet` that have a corresponding spec that
        match all the given spec patterns.
        """
        start_time = time.perf_counter()

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
        logger.debug(
            LazyFormat(lambda: f"Filtering valid linkable elements took: {time.perf_counter() - start_time:.2f}s")
        )
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

    @override
    @property
    def is_empty(self) -> bool:
        return len(self.specs) == 0

    @override
    def intersection(self, *others: LinkableElementSet) -> LinkableElementSet:
        return LinkableElementSet.intersection_by_path_key((self,) + others)

    @override
    def union(self, *others: LinkableElementSet) -> LinkableElementSet:  # noqa: D102
        return LinkableElementSet.merge_by_path_key((self,) + others)
