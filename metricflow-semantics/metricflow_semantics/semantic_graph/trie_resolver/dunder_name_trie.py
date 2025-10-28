from __future__ import annotations

import dataclasses
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from functools import cached_property
from typing import Iterable, Optional, Sequence

from dbt_semantic_interfaces.naming.keywords import DUNDER
from typing_extensions import Self, override

from metricflow_semantics.errors.error_classes import MetricFlowInternalError
from metricflow_semantics.semantic_graph.attribute_resolution.attribute_recipe import IndexedDunderName
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.semantic_graph.trie_resolver.dunder_name_descriptor import DunderNameDescriptor
from metricflow_semantics.toolkit.collections.mapping_helpers import mf_common_keys
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple

logger = logging.getLogger(__name__)


class DunderNameTrie(ABC):
    """A read-only interface for a trie-based data structure that stores dunder names and associated properties.

    Unlike a typical trie that is based on individual characters, this is based on the names that are joined with `__`.
    For example, the trie that contains

        listing__user
        listing__country

    would be represented as:

        listing -> user
                -> country

    The trie helps to simplify union / intersection operations that are done to generate the list of available group-by
    items for a query.

    There's currently a mutable implementation, but a frozen one may be added if it turns out to be useful.
    """

    @abstractmethod
    def name_items(self, max_length: Optional[int] = None) -> Sequence[tuple[IndexedDunderName, DunderNameDescriptor]]:
        """Return a sequence of tuples that represent all dunder names that are represented by this trie.

        The first item is the indexed dunder-name (e.g. ("listing", "country")) and the second is the descriptor
        associated with that name.
        """
        raise NotImplementedError

    def dunder_names(self) -> Sequence[str]:
        """Return the dunder names that are represented by this trie (e.g. ["listing__country", ...]).

        This is mainly used for tests / debugging.
        """
        return tuple(DUNDER.join(indexed_dunder_name) for indexed_dunder_name, _ in self.name_items())

    @classmethod
    @abstractmethod
    def union_merge_common(cls, trie_sequence: Sequence[Self]) -> Self:
        """Union names from a sequence of tries, and merge the descriptors for common names."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def union_exclude_common(cls, trie_sequence: Sequence[Self]) -> Self:
        """Union names from a sequence of tries, but remove the common ones.

        In this operation, non-unique names are discarded. This is to represent ambiguous join paths which are not
        allowed in the query interface.
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def intersection_merge_common(cls, trie_sequence: Sequence[Self]) -> Self:
        """Intersect names from a sequence of tries, and merge the descriptors for common names.

        In an intersection operation, names that do not exist in all tries are discarded. This represents the
        requirement that, for a derived metric, a group-by name must be available for each of the input metrics
        (similar situation for a query of multiple metrics).
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def name_element_to_descriptor(self) -> Mapping[str, DunderNameDescriptor]:
        """Return a mapping of the name elements in this trie to the descriptor.

        For example, this would return `{"listing": Descriptor(...)}` for a trie that stored `listing` and
        `listing__country`. See `name_element_to_next_trie` for retrieval of the `country` element.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def next_name_element_to_trie(self) -> Mapping[str, MutableDunderNameTrie]:
        """Return a mapping from the next name element to the associated trie.

        For example, a trie that represents `listing` and `listing__country` would return
        `{"listing": DunderNameTrie(...)}`.
        """
        raise NotImplementedError

    def mutable_copy(self) -> MutableDunderNameTrie:
        """Return a mutable copy of this trie. Child tries are copied as well.

        Descriptors don't need to be copied as they are immutable.
        """
        return MutableDunderNameTrie(
            name_element_to_descriptor_dict=dict(self.name_element_to_descriptor),
            name_element_to_next_trie_dict={
                name_element: next_trie.mutable_copy()
                for name_element, next_trie in self.next_name_element_to_trie.items()
            },
        )


@dataclass
class MutableDunderNameTrie(DunderNameTrie):
    """A mutable implementation of `DunderNameTrie`."""

    name_element_to_descriptor_dict: dict[str, DunderNameDescriptor] = dataclasses.field(default_factory=dict)
    name_element_to_next_trie_dict: dict[str, MutableDunderNameTrie] = dataclasses.field(
        default_factory=lambda: defaultdict(MutableDunderNameTrie)
    )

    @classmethod
    @override
    def union_merge_common(cls, trie_sequence: Sequence[DunderNameTrie]) -> MutableDunderNameTrie:
        if len(trie_sequence) == 0:
            return MutableDunderNameTrie()
        elif len(trie_sequence) == 1:
            return trie_sequence[0].mutable_copy()

        new_name_element_to_descriptors: dict[str, DunderNameDescriptor] = {}
        for trie in trie_sequence:
            for name_element, descriptor in trie.name_element_to_descriptor.items():
                previous_descriptor = new_name_element_to_descriptors.get(name_element)
                if previous_descriptor is None:
                    new_name_element_to_descriptors[name_element] = descriptor
                else:
                    new_name_element_to_descriptors[name_element] = previous_descriptor.merge(descriptor)

        next_name_element_to_tries_for_union: dict[str, list[MutableDunderNameTrie]] = defaultdict(list)
        for trie in trie_sequence:
            for name_element, next_trie in trie.next_name_element_to_trie.items():
                next_name_element_to_tries_for_union[name_element].append(next_trie)

        new_name_element_to_next_trie: dict[str, MutableDunderNameTrie] = {}
        for name_element, next_tries in next_name_element_to_tries_for_union.items():
            new_name_element_to_next_trie[name_element] = MutableDunderNameTrie.union_merge_common(next_tries)

        return MutableDunderNameTrie(
            name_element_to_descriptor_dict=new_name_element_to_descriptors,
            name_element_to_next_trie_dict=new_name_element_to_next_trie,
        )

    @classmethod
    @override
    def union_exclude_common(cls, trie_sequence: Sequence[DunderNameTrie]) -> MutableDunderNameTrie:
        if len(trie_sequence) == 0:
            return MutableDunderNameTrie()
        elif len(trie_sequence) == 1:
            return trie_sequence[0].mutable_copy()

        common_name_elements_for_descriptors = mf_common_keys(
            tuple(trie.name_element_to_descriptor for trie in trie_sequence)
        )

        new_name_element_to_descriptors: dict[str, DunderNameDescriptor] = {
            name_element: descriptor
            for trie in trie_sequence
            for name_element, descriptor in trie.name_element_to_descriptor.items()
            # if name_element not in new_ambiguous_name_elements
            if name_element not in common_name_elements_for_descriptors
        }

        next_name_element_to_tries_for_union: dict[str, list[MutableDunderNameTrie]] = defaultdict(list)
        for trie in trie_sequence:
            for name_element, next_trie in trie.next_name_element_to_trie.items():
                next_name_element_to_tries_for_union[name_element].append(next_trie)

        new_name_element_to_next_trie: dict[str, MutableDunderNameTrie] = {}
        for name_element, next_tries in next_name_element_to_tries_for_union.items():
            new_name_element_to_next_trie[name_element] = MutableDunderNameTrie.union_exclude_common(next_tries)

        return MutableDunderNameTrie(
            name_element_to_descriptor_dict=new_name_element_to_descriptors,
            name_element_to_next_trie_dict=new_name_element_to_next_trie,
        )

    @classmethod
    @override
    def intersection_merge_common(cls, trie_sequence: Sequence[DunderNameTrie]) -> MutableDunderNameTrie:
        if len(trie_sequence) == 0:
            return MutableDunderNameTrie()
        elif len(trie_sequence) == 1:
            return trie_sequence[0].mutable_copy()

        left_trie = trie_sequence[0]
        other_tries = trie_sequence[1:]

        intersected_name_elements_for_descriptors = mf_common_keys(
            tuple(trie.name_element_to_descriptor for trie in trie_sequence)
        )

        new_element_name_to_descriptor: dict[str, DunderNameDescriptor] = {
            name_element: left_trie.name_element_to_descriptor[name_element]
            for name_element in intersected_name_elements_for_descriptors
        }

        for trie in other_tries:
            for name_element in intersected_name_elements_for_descriptors:
                new_element_name_to_descriptor[name_element] = new_element_name_to_descriptor[name_element].merge(
                    trie.name_element_to_descriptor[name_element]
                )

        intersected_name_elements_for_next_tries = mf_common_keys(
            tuple(trie.next_name_element_to_trie for trie in trie_sequence)
        )

        name_element_to_next_tries: dict[str, list[MutableDunderNameTrie]] = defaultdict(list)
        for trie in trie_sequence:
            for name_element, next_trie in trie.next_name_element_to_trie.items():
                if name_element not in intersected_name_elements_for_next_tries:
                    continue
                name_element_to_next_tries[name_element].append(next_trie)

        new_name_element_to_next_trie = {
            name_element: MutableDunderNameTrie.intersection_merge_common(next_tries)
            for name_element, next_tries in name_element_to_next_tries.items()
        }

        return MutableDunderNameTrie(
            name_element_to_descriptor_dict=new_element_name_to_descriptor,
            name_element_to_next_trie_dict=new_name_element_to_next_trie,
        )

    @cached_property
    @override
    def name_element_to_descriptor(self) -> Mapping[str, DunderNameDescriptor]:
        return self.name_element_to_descriptor_dict

    @cached_property
    @override
    def next_name_element_to_trie(self) -> Mapping[str, MutableDunderNameTrie]:
        return self.name_element_to_next_trie_dict

    def update_derived_from_model_ids(self, derived_from_model_ids: AnyLengthTuple[SemanticModelId]) -> None:
        """Update the descriptor for all represented names to include additional `derived_from_model_ids`."""
        self.name_element_to_descriptor_dict.update(
            {
                name_element: descriptor.merge_derived_from_model_ids(derived_from_model_ids)
                for name_element, descriptor in self.name_element_to_descriptor_dict.items()
            }
        )
        for _, next_trie in self.name_element_to_next_trie_dict.items():
            next_trie.update_derived_from_model_ids(derived_from_model_ids)

    def add_name_items(self, items: Iterable[tuple[IndexedDunderName, DunderNameDescriptor]]) -> None:
        """Build out this trie by adding individual names."""
        # Mapping from the path to the leaf trie to the common name elements.
        # e.g. for items
        #
        #       `listing__country`,
        #       `listing__country`,
        #       `listing__country__region`
        #
        # This should contain:
        # {
        #     (`listing`,): {
        #         "country",
        #     },
        # }
        trie_path_to_common_names: dict[IndexedDunderName, set[str]] = defaultdict(set)
        for indexed_dunder_name, descriptor in items:
            name_element_count = len(indexed_dunder_name)
            if name_element_count == 0:
                raise MetricFlowInternalError(
                    LazyFormat(
                        "There must at least be one name element.",
                        name_element_list=indexed_dunder_name,
                    )
                )
            leaf_trie_path = indexed_dunder_name[:-1]

            # Traverse the trie tree to find the leaf trie.
            current_trie = self
            for name_element_index in range(name_element_count):
                current_name_element = indexed_dunder_name[name_element_index]

                # At the leaf.
                if name_element_index == name_element_count - 1:
                    name_element_to_descriptor = current_trie.name_element_to_descriptor_dict
                    current_descriptor = name_element_to_descriptor.get(current_name_element)

                    # If there are multiple names given, they are considered ambiguous.
                    # Ambiguous names should be removed.
                    if current_name_element in trie_path_to_common_names[leaf_trie_path]:
                        continue

                    if current_descriptor is None:
                        name_element_to_descriptor[current_name_element] = descriptor
                        continue

                    del name_element_to_descriptor[current_name_element]
                    trie_path_to_common_names[leaf_trie_path].add(current_name_element)
                    continue

                next_trie = current_trie.name_element_to_next_trie_dict[current_name_element]
                current_trie.name_element_to_next_trie_dict[current_name_element] = next_trie

                current_trie = next_trie

    @override
    def name_items(self, max_length: Optional[int] = None) -> Sequence[tuple[IndexedDunderName, DunderNameDescriptor]]:
        item_collector: list[tuple[IndexedDunderName, DunderNameDescriptor]] = []
        self._collect_name_items(
            name_element_prefix=(), item_collector=item_collector, current_element_index=0, max_element_count=max_length
        )
        return item_collector

    def _collect_name_items(
        self,
        name_element_prefix: IndexedDunderName,
        item_collector: list[tuple[IndexedDunderName, DunderNameDescriptor]],
        current_element_index: int,
        max_element_count: Optional[int],
    ) -> None:
        """Recursive helper method to add names represented in the trie to `item_collector`.

        If `max_element_count` is set, names with more elements than the given value are not included.
        """
        if max_element_count is not None and current_element_index + 1 > max_element_count:
            return

        item_collector.extend(
            (name_element_prefix + (name_element,), descriptor)
            for name_element, descriptor in self.name_element_to_descriptor_dict.items()
        )

        for name_element, next_node in self.name_element_to_next_trie_dict.items():
            next_node._collect_name_items(
                name_element_prefix + (name_element,), item_collector, current_element_index + 1, max_element_count
            )
