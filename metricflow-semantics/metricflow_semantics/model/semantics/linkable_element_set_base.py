from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from functools import cached_property
from typing import Sequence

from dbt_semantic_interfaces.references import SemanticModelReference
from typing_extensions import Self, override

from metricflow_semantics.collection_helpers.merger import Mergeable
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantic_model_derivation import SemanticModelDerivation
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
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


@fast_frozen_dataclass()
class AnnotatedSpec(SemanticModelDerivation):
    element_type: LinkableElementType
    spec: LinkableInstanceSpec
    properties: FrozenOrderedSet[LinkableElementProperty]
    # The semantic model(s) where the element (e.g. the categorical dimension) was defined.
    # There can be multiple models if it's a metric / derived metric that references multiple measures, and the join
    # path from the measure to the dimension is different, but using a singular item during migration to align with the
    # existing code.
    origin_model_id: SemanticModelId
    _derived_from_semantic_models: FrozenOrderedSet[SemanticModelReference]

    @staticmethod
    def create(
        element_type: LinkableElementType,
        spec: LinkableInstanceSpec,
        properties: FrozenOrderedSet[LinkableElementProperty],
        origin_model: SemanticModelId,
        derived_from_semantic_models: FrozenOrderedSet[SemanticModelReference],
    ) -> AnnotatedSpec:
        return AnnotatedSpec(
            element_type=element_type,
            spec=spec,
            properties=properties,
            origin_model_id=origin_model,
            _derived_from_semantic_models=derived_from_semantic_models,
        )

    @override
    @cached_property
    def derived_from_semantic_models(self) -> Sequence[SemanticModelReference]:
        return tuple(self._derived_from_semantic_models)
