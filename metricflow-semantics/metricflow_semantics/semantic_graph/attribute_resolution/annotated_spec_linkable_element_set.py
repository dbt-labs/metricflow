from __future__ import annotations

import logging
from collections.abc import Sequence
from functools import cached_property
from typing import Mapping, Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from dbt_semantic_interfaces.references import SemanticModelReference
from typing_extensions import override

from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
from metricflow_semantics.model.semantics.linkable_element_set_base import AnnotatedSpec, BaseGroupByItemSet
from metricflow_semantics.semantic_graph.trie_resolver.dunder_name_trie import DunderNameTrie
from metricflow_semantics.specs.instance_spec import InstanceSpec, LinkableInstanceSpec
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, MutableOrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.toolkit.syntactic_sugar import mf_flatten

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class GroupByItemSet(BaseGroupByItemSet, SerializableDataclass):
    """Implementation of `BaseGroupByItemSet` based on `AnnotatedSpec`."""

    annotated_specs: Tuple[AnnotatedSpec, ...] = ()

    @staticmethod
    def create(  # noqa: D102
        *annotated_specs: AnnotatedSpec,
    ) -> GroupByItemSet:
        dunder_name_to_annotated_spec: dict[str, AnnotatedSpec] = {}
        for annotated_spec in annotated_specs:
            dunder_name = annotated_spec.spec.dunder_name
            existing_annotated_spec = dunder_name_to_annotated_spec.get(dunder_name)
            if existing_annotated_spec is None:
                dunder_name_to_annotated_spec[dunder_name] = annotated_spec
            else:
                dunder_name_to_annotated_spec[dunder_name] = existing_annotated_spec.merge(annotated_spec)

        return GroupByItemSet(
            annotated_specs=tuple(dunder_name_to_annotated_spec.values()),
        )

    @staticmethod
    def create_from_trie(*dunder_name_tries: DunderNameTrie) -> GroupByItemSet:  # noqa: D102
        return GroupByItemSet(
            annotated_specs=tuple(
                annotated_spec
                for annotated_spec in mf_flatten(
                    AnnotatedSpec.create_from_indexed_dunder_name(indexed_dunder_name, descriptor)
                    for trie in dunder_name_tries
                    for indexed_dunder_name, descriptor in trie.name_items()
                )
            )
        )

    @staticmethod
    def create_from_mapping(dunder_name_to_annotated_spec: dict[str, AnnotatedSpec]) -> GroupByItemSet:  # noqa: D102
        return GroupByItemSet(annotated_specs=tuple(dunder_name_to_annotated_spec.values()))

    @cached_property
    def dunder_name_to_annotated_spec(self) -> Mapping[str, AnnotatedSpec]:
        """Return a mapping from the dunder name to the annotated spec."""
        return {annotated_spec.spec.dunder_name: annotated_spec for annotated_spec in self.annotated_specs}

    @override
    def intersection(self, *other_element_sets: GroupByItemSet) -> GroupByItemSet:
        if len(other_element_sets) == 0:
            return self

        common_keys = MutableOrderedSet(self.dunder_name_to_annotated_spec.keys())
        common_keys.intersection_update(
            *(element_set.dunder_name_to_annotated_spec.keys() for element_set in other_element_sets)
        )

        intersected_dunder_name_to_annotated_spec: dict[str, AnnotatedSpec] = {}
        for common_key in common_keys:
            annotated_spec = self.dunder_name_to_annotated_spec[common_key]
            for other_element_set in other_element_sets:
                annotated_spec = annotated_spec.merge(other_element_set.dunder_name_to_annotated_spec[common_key])
            intersected_dunder_name_to_annotated_spec[common_key] = annotated_spec

        return GroupByItemSet.create_from_mapping(intersected_dunder_name_to_annotated_spec)

    @override
    def union(self, *others: GroupByItemSet) -> GroupByItemSet:
        new_dunder_name_to_annotated_spec: dict[str, AnnotatedSpec] = dict(**self.dunder_name_to_annotated_spec)

        for other in others:
            for dunder_name, annotated_spec in other.dunder_name_to_annotated_spec.items():
                current_annotated_spec = new_dunder_name_to_annotated_spec.get(dunder_name)
                if current_annotated_spec is None:
                    new_dunder_name_to_annotated_spec[dunder_name] = annotated_spec
                else:
                    new_dunder_name_to_annotated_spec[dunder_name] = current_annotated_spec.merge(annotated_spec)

                new_dunder_name_to_annotated_spec[dunder_name] = annotated_spec

        return GroupByItemSet.create_from_mapping(new_dunder_name_to_annotated_spec)

    @override
    def filter(self, element_filter: GroupByItemSetFilter) -> GroupByItemSet:
        element_name_allowlist = element_filter.element_name_allowlist
        any_properties_denylist = element_filter.any_properties_denylist

        new_specs: list[AnnotatedSpec] = []
        for annotated_spec in self.annotated_specs:
            if element_name_allowlist is not None and annotated_spec.element_name not in element_name_allowlist:
                continue

            property_set = annotated_spec.property_set

            if not element_filter.any_properties_allowlist.intersection(annotated_spec.property_set):
                continue

            if any_properties_denylist and len(any_properties_denylist.intersection(property_set)) > 0:
                continue

            new_specs.append(annotated_spec)

        return GroupByItemSet(annotated_specs=tuple(new_specs))

    @override
    @property
    def is_empty(self) -> bool:
        return len(self.annotated_specs) == 0

    @override
    @cached_property
    def specs(self) -> AnyLengthTuple[LinkableInstanceSpec]:
        return tuple(annotated_spec.spec for annotated_spec in self.annotated_specs)

    @override
    def filter_by_spec_patterns(self, spec_patterns: Sequence[SpecPattern]) -> GroupByItemSet:
        if len(spec_patterns) == 0:
            return self

        spec_to_annotated_spec: dict[InstanceSpec, AnnotatedSpec] = {
            annotated_spec.spec: annotated_spec for annotated_spec in self.annotated_specs
        }
        specs = tuple(spec_to_annotated_spec.keys())
        for spec_pattern in spec_patterns:
            matched_specs = tuple(spec_pattern.match(specs))
            specs = matched_specs

        return GroupByItemSet(annotated_specs=tuple(spec_to_annotated_spec[matched_spec] for matched_spec in specs))

    @override
    @cached_property
    def derived_from_semantic_models(self) -> Sequence[SemanticModelReference]:
        return tuple(
            FrozenOrderedSet(
                mf_flatten(annotated_spec.derived_from_semantic_models for annotated_spec in self.annotated_specs)
            )
        )
