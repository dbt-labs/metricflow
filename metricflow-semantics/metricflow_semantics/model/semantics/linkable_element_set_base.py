from __future__ import annotations

import itertools
import logging
import typing
from abc import ABC, abstractmethod
from functools import cached_property
from typing import Iterable, Optional, Sequence, Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.references import EntityReference, SemanticModelReference
from dbt_semantic_interfaces.type_enums import DatePart
from typing_extensions import Self

from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
from metricflow_semantics.model.semantic_model_derivation import SemanticModelDerivation
from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.time.granularity import ExpandedTimeGranularity
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple

if typing.TYPE_CHECKING:
    from metricflow_semantics.semantic_graph.attribute_resolution.attribute_recipe import IndexedDunderName
    from metricflow_semantics.semantic_graph.trie_resolver.dunder_name_descriptor import (
        DunderNameDescriptor,
    )

logger = logging.getLogger(__name__)


class BaseGroupByItemSet(SemanticModelDerivation, ABC):
    """ABC for a set that represents possible group-by items."""

    @abstractmethod
    def filter(
        self,
        element_filter: GroupByItemSetFilter,
    ) -> Self:
        """Filter elements in the set.

        First, only elements with at least one property in the "any_properties_allowlist" set are retained. Then, any elements with
        a property in "any_properties_denylist" set are removed.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def is_empty(self) -> bool:
        """Whether this maps to an empty set of specs.

        If `.specs` is created lazily, there are cases where the caller only needs to know that there are matching
        specs.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def specs(self) -> Sequence[LinkableInstanceSpec]:
        """Converts the items represented by this to the corresponding spec objects."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def annotated_specs(self) -> Sequence[AnnotatedSpec]:
        """Return the annotated specs represented by this."""
        raise NotImplementedError

    @abstractmethod
    def filter_by_spec_patterns(self, spec_patterns: Sequence[SpecPattern]) -> Self:
        """Filter the elements in the set by the given spec patters.

        Returns a new set consisting of the elements in the `LinkableElementSet` that have a corresponding spec that
        match all the given spec patterns.
        """
        raise NotImplementedError()

    @abstractmethod
    def intersection(self, *others: Self) -> Self:
        """Intersect this set with the other set to determine possible group-by items."""
        raise NotImplementedError

    @abstractmethod
    def union(self, *others: Self) -> Self:
        """Intersect this set with the other set to determine possible group-by items."""
        raise NotImplementedError


@fast_frozen_dataclass()
class AnnotatedSpec(SerializableDataclass):
    """Groups a spec with more context about the spec.

    * This is intended to consolidate the `LinkableElement` classes during migration.
    * This class will be updated / renamed.
    """

    element_type: LinkableElementType
    element_name: str
    entity_link_names: Tuple[str, ...]
    time_grain: Optional[ExpandedTimeGranularity]
    date_part: Optional[DatePart]
    metric_subquery_entity_link_names: Tuple[str, ...]

    element_properties: Tuple[GroupByItemProperty, ...]
    # The semantic model(s) where the element (e.g. the categorical dimension) was defined.
    # There can be multiple models if it's a metric / derived metric that references multiple simple-metric inputs, and the join
    # path from the simple-metric input to the dimension is different.
    origin_semantic_model_names: Tuple[str, ...]
    derived_from_semantic_model_names: Tuple[str, ...]

    @staticmethod
    def create(  # noqa: D102
        element_type: LinkableElementType,
        element_name: str,
        properties: Iterable[GroupByItemProperty],
        origin_model_ids: Iterable[SemanticModelId],
        derived_from_semantic_models: Iterable[SemanticModelReference],
        entity_links: Sequence[EntityReference],
        metric_subquery_entity_links: Optional[Sequence[EntityReference]],
        time_grain: Optional[ExpandedTimeGranularity],
        date_part: Optional[DatePart],
    ) -> AnnotatedSpec:
        entity_link_names = tuple(entity_reference.element_name for entity_reference in entity_links)
        element_properties = tuple(FrozenOrderedSet(properties))
        origin_model_names = tuple(FrozenOrderedSet(model_id.model_name for model_id in origin_model_ids))
        derived_from_semantic_model_names = tuple(
            FrozenOrderedSet(model_reference.semantic_model_name for model_reference in derived_from_semantic_models)
        )
        metric_subquery_entity_link_names = (
            tuple(entity_reference.element_name for entity_reference in metric_subquery_entity_links)
            if metric_subquery_entity_links is not None
            else ()
        )

        return AnnotatedSpec(
            element_type=element_type,
            element_name=element_name,
            entity_link_names=entity_link_names,
            element_properties=element_properties,
            time_grain=time_grain,
            date_part=date_part,
            metric_subquery_entity_link_names=metric_subquery_entity_link_names,
            origin_semantic_model_names=origin_model_names,
            derived_from_semantic_model_names=derived_from_semantic_model_names,
        )

    @staticmethod
    def create_from_indexed_dunder_name(  # noqa: D102
        indexed_dunder_name: IndexedDunderName, descriptor: DunderNameDescriptor
    ) -> Sequence[AnnotatedSpec]:
        items: list[AnnotatedSpec] = []

        element_type = descriptor.element_type

        if (
            element_type is LinkableElementType.DIMENSION
            or element_type is LinkableElementType.ENTITY
            or element_type is LinkableElementType.METRIC
        ):
            element_name = indexed_dunder_name[-1]
            entity_links = tuple(EntityReference(name_element) for name_element in indexed_dunder_name[:-1])
        elif element_type is LinkableElementType.TIME_DIMENSION:
            element_name = indexed_dunder_name[-2]
            entity_links = tuple(EntityReference(name_element) for name_element in indexed_dunder_name[:-2])
        else:
            assert_values_exhausted(element_type)

        if (
            element_type is LinkableElementType.DIMENSION
            or element_type is LinkableElementType.ENTITY
            or element_type is LinkableElementType.TIME_DIMENSION
        ):
            items.append(
                AnnotatedSpec.create(
                    element_type=descriptor.element_type,
                    element_name=element_name,
                    properties=descriptor.element_properties,
                    origin_model_ids=descriptor.origin_model_ids,
                    derived_from_semantic_models=(
                        model_id.semantic_model_reference for model_id in descriptor.derived_from_model_ids
                    ),
                    entity_links=entity_links,
                    metric_subquery_entity_links=None,
                    time_grain=descriptor.time_grain,
                    date_part=descriptor.date_part,
                )
            )
        elif element_type is LinkableElementType.METRIC:
            for entity_key_query in descriptor.entity_key_queries_for_group_by_metric:
                items.append(
                    AnnotatedSpec.create(
                        element_type=descriptor.element_type,
                        element_name=element_name,
                        properties=descriptor.element_properties,
                        origin_model_ids=descriptor.origin_model_ids,
                        derived_from_semantic_models=FrozenOrderedSet(
                            itertools.chain(
                                (model_id.semantic_model_reference for model_id in descriptor.derived_from_model_ids),
                                (
                                    model_id.semantic_model_reference
                                    for model_id in entity_key_query.derived_from_model_ids
                                ),
                            )
                        ),
                        entity_links=entity_links,
                        metric_subquery_entity_links=tuple(
                            EntityReference(name_element) for name_element in entity_key_query.entity_key_query
                        ),
                        time_grain=None,
                        date_part=None,
                    )
                )
        else:
            assert_values_exhausted(element_type)

        return items

    @cached_property
    def spec(self) -> LinkableInstanceSpec:
        """Return the type-specific spec object.

        This should be renamed to avoid confusion.
        """
        element_type = self.element_type
        element_name = self.element_name
        entity_links = tuple(EntityReference(entity_link_name) for entity_link_name in self.entity_link_names)
        if element_type is LinkableElementType.METRIC:
            metric_subquery_entity_link_names = self.metric_subquery_entity_link_names
            assert metric_subquery_entity_link_names is not None
            metric_subquery_entity_links = tuple(
                EntityReference(entity_link_name) for entity_link_name in metric_subquery_entity_link_names
            )
            return GroupByMetricSpec(
                element_name=element_name,
                entity_links=entity_links,
                metric_subquery_entity_links=metric_subquery_entity_links,
            )
        elif element_type is LinkableElementType.TIME_DIMENSION:
            return TimeDimensionSpec(
                element_name=element_name,
                entity_links=entity_links,
                time_granularity=self.time_grain,
                date_part=self.date_part,
            )
        elif element_type is LinkableElementType.DIMENSION:
            return DimensionSpec(
                element_name=element_name,
                entity_links=entity_links,
            )
        elif element_type is LinkableElementType.ENTITY:
            return EntitySpec(
                element_name=self.element_name,
                entity_links=entity_links,
            )
        elif element_type is LinkableElementType.TIME_DIMENSION:
            return TimeDimensionSpec(
                element_name=element_name,
                entity_links=entity_links,
                time_granularity=self.time_grain,
                date_part=self.date_part,
            )
        else:
            assert_values_exhausted(element_type)

    @cached_property
    def property_set(self) -> FrozenOrderedSet[GroupByItemProperty]:  # noqa: D102
        return FrozenOrderedSet(self.element_properties)

    @cached_property
    def origin_model_ids(self) -> FrozenOrderedSet[SemanticModelId]:  # noqa: D102
        return FrozenOrderedSet(
            SemanticModelId.get_instance(model_name) for model_name in self.origin_semantic_model_names
        )

    @cached_property
    def derived_from_semantic_models(self) -> Sequence[SemanticModelReference]:  # noqa: D102
        return tuple(SemanticModelReference(model_name) for model_name in self.derived_from_semantic_model_names)

    def merge(self, other: AnnotatedSpec) -> AnnotatedSpec:  # noqa: D102
        if (
            self.element_type != other.element_type
            or self.element_name != other.element_name
            or self.time_grain != other.time_grain
            or self.date_part != other.date_part
            or self.metric_subquery_entity_link_names != other.metric_subquery_entity_link_names
        ):
            raise RuntimeError(
                LazyFormat(
                    "Unable to merge annotated specs due to incompatible fields. This indicates an error in"
                    " the set of annotated specs being merged.",
                    self=self,
                    other=other,
                )
            )

        return AnnotatedSpec(
            element_type=self.element_type,
            element_name=self.element_name,
            entity_link_names=self.entity_link_names,
            time_grain=self.time_grain,
            date_part=self.date_part,
            metric_subquery_entity_link_names=self.metric_subquery_entity_link_names,
            element_properties=tuple(self.property_set.union(other.property_set)),
            origin_semantic_model_names=tuple(
                FrozenOrderedSet(self.origin_semantic_model_names + other.origin_semantic_model_names)
            ),
            derived_from_semantic_model_names=tuple(
                FrozenOrderedSet(self.derived_from_semantic_model_names + other.derived_from_semantic_model_names)
            ),
        )

    @cached_property
    def origin_semantic_model_references(self) -> AnyLengthTuple[SemanticModelReference]:
        """Returns the models where the element is defined.

        There can be multiple as a derived metric may have a group-by item that can be reached via multiple join paths.
        """
        return tuple(model_id.semantic_model_reference for model_id in self.origin_model_ids)
