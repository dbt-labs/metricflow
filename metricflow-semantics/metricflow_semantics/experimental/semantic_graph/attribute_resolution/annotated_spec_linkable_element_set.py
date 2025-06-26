from __future__ import annotations

import logging
from collections.abc import Sequence
from functools import cached_property

from dbt_semantic_interfaces.references import SemanticModelReference
from typing_extensions import Self, override

from metricflow_semantics.collection_helpers.syntactic_sugar import mf_flatten
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_element_set_base import AnnotatedSpec, BaseLinkableElementSet
from metricflow_semantics.specs.instance_spec import InstanceSpec, LinkableInstanceSpec
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern

logger = logging.getLogger(__name__)


class AnnotatedSpecLinkableElementSet(BaseLinkableElementSet):
    """Temporary implementation of `BaseLinkableElementSet` based on `AnnotatedSpec`."""

    def __init__(  # noqa: D107
        self,
        annotated_specs: FrozenOrderedSet[AnnotatedSpec],
    ) -> None:
        self._annotated_specs = annotated_specs

    @override
    def filter(self, element_filter: LinkableElementFilter) -> AnnotatedSpecLinkableElementSet:
        element_names = element_filter.element_names
        with_any_of = element_filter.with_any_of
        without_any_of = element_filter.without_any_of
        without_all_of = element_filter.without_all_of

        matching_annotated_specs: list[AnnotatedSpec] = []

        for annotated_spec in self.annotated_specs:
            spec = annotated_spec.spec
            properties = annotated_spec.properties

            if element_names is not None and spec.element_name not in element_names:
                continue

            if (
                len(properties.intersection(with_any_of)) > 0
                and len(properties.intersection(without_any_of)) == 0
                and (len(without_all_of) == 0 or properties.intersection(without_all_of) != without_all_of)
            ):
                matching_annotated_specs.append(annotated_spec)

        return AnnotatedSpecLinkableElementSet(
            annotated_specs=FrozenOrderedSet(matching_annotated_specs),
        )

    @override
    @property
    def is_empty(self) -> bool:
        return len(self.annotated_specs) == 0

    @override
    @cached_property
    def specs(self) -> Sequence[LinkableInstanceSpec]:
        return tuple(annotated_spec.spec for annotated_spec in self._annotated_specs)

    @override
    @cached_property
    def annotated_specs(self) -> Sequence[AnnotatedSpec]:
        return tuple(self._annotated_specs)

    @override
    def filter_by_spec_patterns(self, spec_patterns: Sequence[SpecPattern]) -> AnnotatedSpecLinkableElementSet:
        current_specs_matching_patterns: Sequence[InstanceSpec] = self.specs
        for spec_pattern in spec_patterns:
            current_specs_matching_patterns = spec_pattern.match(current_specs_matching_patterns)
        specs_to_include = FrozenOrderedSet(current_specs_matching_patterns)

        return AnnotatedSpecLinkableElementSet(
            FrozenOrderedSet(
                annotated_spec for annotated_spec in self._annotated_specs if annotated_spec.spec in specs_to_include
            )
        )

    @override
    @cached_property
    def derived_from_semantic_models(self) -> Sequence[SemanticModelReference]:
        return tuple(
            FrozenOrderedSet(
                mf_flatten(annotated_spec.derived_from_semantic_models for annotated_spec in self._annotated_specs)
            )
        )

    @override
    def merge(self: Self, other: AnnotatedSpecLinkableElementSet) -> AnnotatedSpecLinkableElementSet:
        # TODO: This might not be needed
        raise NotImplementedError()

    @classmethod
    @override
    def empty_instance(cls) -> AnnotatedSpecLinkableElementSet:
        return AnnotatedSpecLinkableElementSet(FrozenOrderedSet())
