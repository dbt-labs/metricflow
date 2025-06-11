from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from functools import cached_property
from typing import Sequence

from dbt_semantic_interfaces.references import SemanticModelReference
from typing_extensions import Self, override

from metricflow_semantics.collection_helpers.merger import Mergeable
from metricflow_semantics.collection_helpers.syntactic_sugar import mf_flatten
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantic_model_derivation import SemanticModelDerivation
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_element import LinkableElement
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern

logger = logging.getLogger(__name__)


class BaseLinkableElementSet(SemanticModelDerivation, Mergeable, ABC):
    """Temporary interface used to migrate `LinkableElementSet`."""

    @abstractmethod
    def filter(
        self,
        element_filter: LinkableElementFilter,
    ) -> Self:
        """Filter elements in the set.

        First, only elements with at least one property in the "with_any_of" set are retained. Then, any elements with
        a property in "without_any_of" set are removed. Lastly, any elements with all properties in without_all_of
        are removed.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def is_empty(self) -> bool:
        """Whether this maps to an empty set of specs.."""
        raise NotImplementedError

    @property
    @abstractmethod
    def specs(self) -> Sequence[LinkableInstanceSpec]:
        """Converts the items represented by this to the corresponding spec objects."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def annotated_specs(self) -> Sequence[AnnotatedSpec]:
        raise NotImplementedError

    @abstractmethod
    def filter_by_spec_patterns(self, spec_patterns: Sequence[SpecPattern]) -> Self:
        """Filter the elements in the set by the given spec patters.

        Returns a new set consisting of the elements in the `LinkableElementSet` that have a corresponding spec that
        match all the given spec patterns.
        """
        raise NotImplementedError()

    @property
    def linkable_elements(self) -> Sequence[LinkableElement]:
        return tuple(mf_flatten(annotated_spec.linkable_element for annotated_spec in self.annotated_specs))


@fast_frozen_dataclass()
class AnnotatedSpec(SemanticModelDerivation):
    spec: LinkableInstanceSpec
    properties: FrozenOrderedSet[LinkableElementProperty]
    # path_key: ElementPathKey
    # linkable_element: Optional[LinkableElement]
    _derived_from_semantic_models: FrozenOrderedSet[SemanticModelReference]

    @staticmethod
    def create(
        spec: LinkableInstanceSpec,
        properties: FrozenOrderedSet[LinkableElementProperty],
        # path_key: ElementPathKey,
        # linkable_element: Optional[LinkableElement],
        derived_from_semantic_models: FrozenOrderedSet[SemanticModelReference],
    ) -> AnnotatedSpec:
        return AnnotatedSpec(
            spec=spec,
            properties=properties,
            # path_key=path_key,
            # linkable_element=linkable_element,
            _derived_from_semantic_models=derived_from_semantic_models,
        )

    @override
    @cached_property
    def derived_from_semantic_models(self) -> Sequence[SemanticModelReference]:
        return tuple(self._derived_from_semantic_models)
