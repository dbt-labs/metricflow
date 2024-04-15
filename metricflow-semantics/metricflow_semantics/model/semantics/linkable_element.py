from __future__ import annotations

import logging
from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import FrozenSet, Optional, Sequence, Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols.dimension import DimensionType
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    MetricReference,
    SemanticModelReference,
)
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from typing_extensions import override

from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantic_model_derivation import SemanticModelDerivation
from metricflow_semantics.specs.spec_classes import (
    DimensionSpec,
    EntitySpec,
    GroupByMetricSpec,
    LinkableInstanceSpec,
    TimeDimensionSpec,
)

logger = logging.getLogger(__name__)


class LinkableElementType(Enum):
    """Enumeration of the possible types of linkable element we are encountering or expecting.

    LinkableElements effectively map on to LinkableSpecs and queryable semantic manifest elements such
    as Metrics, Dimensions, and Entities. This provides the full set of types we might encounter, and is
    useful for ensuring that we are always getting the correct LinkableElement from a given part of the
    codebase - e.g., to ensure we are not accidentally getting an Entity when we expect a Dimension.
    """

    DIMENSION = "dimension"
    ENTITY = "entity"
    METRIC = "metric"
    TIME_DIMENSION = "time_dimension"

    @property
    def is_dimension_type(self) -> bool:
        """Property to simplify scenarios where callers need to know whether or not this represents a dimension."""
        # Use a local alias to allow type refinement for the static exhaustive switch assertion
        element_type = self
        if element_type is LinkableElementType.DIMENSION or element_type is LinkableElementType.TIME_DIMENSION:
            return True
        elif element_type is LinkableElementType.ENTITY or element_type is LinkableElementType.METRIC:
            return False
        else:
            return assert_values_exhausted(element_type)


@dataclass(frozen=True)
class ElementPathKey:
    """A key that can uniquely identify an element and the joins used to realize the element."""

    element_name: str
    element_type: LinkableElementType
    entity_links: Tuple[EntityReference, ...]
    time_granularity: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None

    def __post_init__(self) -> None:
        """Asserts all requirements associated with the element_type are met."""
        element_type = self.element_type
        if element_type is LinkableElementType.TIME_DIMENSION:
            assert (
                self.time_granularity
            ), "Time granularity must be specified for all ElementPathKeys associated with time dimensions!"
        elif element_type is LinkableElementType.DIMENSION or LinkableElementType.ENTITY or LinkableElementType.METRIC:
            pass
        else:
            assert_values_exhausted(element_type)

    @property
    def spec(self) -> LinkableInstanceSpec:
        """The corresponding spec object for this path key."""
        if self.element_type is LinkableElementType.DIMENSION:
            return DimensionSpec(
                element_name=self.element_name,
                entity_links=self.entity_links,
            )
        elif self.element_type is LinkableElementType.TIME_DIMENSION:
            assert (
                self.time_granularity is not None
            ), f"{self.time_granularity=} should not be None as per check in dataclass validation"
            return TimeDimensionSpec(
                element_name=self.element_name,
                entity_links=self.entity_links,
                time_granularity=self.time_granularity,
                date_part=self.date_part,
            )
        elif self.element_type is LinkableElementType.ENTITY:
            return EntitySpec(
                element_name=self.element_name,
                entity_links=self.entity_links,
            )
        elif self.element_type is LinkableElementType.METRIC:
            return GroupByMetricSpec(
                element_name=self.element_name,
                entity_links=self.entity_links,
            )
        else:
            assert_values_exhausted(self.element_type)


@dataclass(frozen=True)
class SemanticModelJoinPathElement:
    """Describes joining a semantic model by the given entity."""

    semantic_model_reference: SemanticModelReference
    join_on_entity: EntityReference


class LinkableElement(SemanticModelDerivation, ABC):
    """An entity / dimension that may have been joined by entities."""

    pass


@dataclass(frozen=True)
class LinkableDimension(LinkableElement):
    """Describes how a dimension can be realized by joining based on entity links."""

    # The semantic model where this dimension was defined.
    semantic_model_origin: Optional[SemanticModelReference]
    element_name: str
    dimension_type: DimensionType
    entity_links: Tuple[EntityReference, ...]
    join_path: Tuple[SemanticModelJoinPathElement, ...]
    properties: FrozenSet[LinkableElementProperty]
    time_granularity: Optional[TimeGranularity]
    date_part: Optional[DatePart]

    @property
    def path_key(self) -> ElementPathKey:  # noqa: D102
        if self.dimension_type is DimensionType.CATEGORICAL:
            element_type = LinkableElementType.DIMENSION
        else:
            element_type = LinkableElementType.TIME_DIMENSION

        return ElementPathKey(
            element_name=self.element_name,
            element_type=element_type,
            entity_links=self.entity_links,
            time_granularity=self.time_granularity,
            date_part=self.date_part,
        )

    @property
    def reference(self) -> DimensionReference:  # noqa: D102
        return DimensionReference(element_name=self.element_name)

    @property
    @override
    def derived_from_semantic_models(self) -> Sequence[SemanticModelReference]:
        semantic_model_references = set()
        if self.semantic_model_origin:
            semantic_model_references.add(self.semantic_model_origin)
        for join_path_item in self.join_path:
            semantic_model_references.add(join_path_item.semantic_model_reference)

        return sorted(semantic_model_references, key=lambda reference: reference.semantic_model_name)


@dataclass(frozen=True)
class LinkableEntity(LinkableElement):
    """Describes how an entity can be realized by joining based on entity links."""

    # The semantic model where this entity was defined.
    semantic_model_origin: SemanticModelReference
    element_name: str
    properties: FrozenSet[LinkableElementProperty]
    entity_links: Tuple[EntityReference, ...]
    join_path: Tuple[SemanticModelJoinPathElement, ...]

    @property
    def path_key(self) -> ElementPathKey:  # noqa: D102
        return ElementPathKey(
            element_name=self.element_name, element_type=LinkableElementType.ENTITY, entity_links=self.entity_links
        )

    @property
    def reference(self) -> EntityReference:  # noqa: D102
        return EntityReference(element_name=self.element_name)

    @property
    @override
    def derived_from_semantic_models(self) -> Sequence[SemanticModelReference]:
        semantic_model_references = {self.semantic_model_origin}
        for join_path_item in self.join_path:
            semantic_model_references.add(join_path_item.semantic_model_reference)

        return sorted(semantic_model_references, key=lambda reference: reference.semantic_model_name)


@dataclass(frozen=True)
class LinkableMetric(LinkableElement):
    """Describes how a metric can be realized by joining based on entity links."""

    element_name: str
    join_by_semantic_model: SemanticModelReference
    # TODO: Enable joining by dimension
    entity_links: Tuple[EntityReference, ...]
    properties: FrozenSet[LinkableElementProperty]
    join_path: Tuple[SemanticModelJoinPathElement, ...]

    @property
    def path_key(self) -> ElementPathKey:  # noqa: D102
        return ElementPathKey(
            element_name=self.element_name, element_type=LinkableElementType.METRIC, entity_links=self.entity_links
        )

    @property
    def reference(self) -> MetricReference:  # noqa: D102
        return MetricReference(element_name=self.element_name)

    @property
    @override
    def derived_from_semantic_models(self) -> Sequence[SemanticModelReference]:
        semantic_model_references = {self.join_by_semantic_model}
        for join_path_item in self.join_path:
            semantic_model_references.add(join_path_item.semantic_model_reference)

        return sorted(semantic_model_references, key=lambda reference: reference.semantic_model_name)


@dataclass(frozen=True)
class SemanticModelJoinPath:
    """Describes a series of joins between the measure semantic model, and other semantic models by entity.

    For example:

    (measure_source JOIN dimension_source0 ON entity A) JOIN dimension_source1 ON entity B

    would be represented by 2 path elements [(semantic_model0, A), (dimension_source1, B)]
    """

    path_elements: Tuple[SemanticModelJoinPathElement, ...]

    @property
    def last_path_element(self) -> SemanticModelJoinPathElement:  # noqa: D102
        assert len(self.path_elements) > 0
        return self.path_elements[-1]

    @property
    def last_semantic_model_reference(self) -> SemanticModelReference:
        """The last semantic model that would be joined in this path."""
        return self.last_path_element.semantic_model_reference

    @property
    def last_entity_link(self) -> EntityReference:  # noqa: D102
        return self.last_path_element.join_on_entity

    @property
    def entity_links(self) -> Tuple[EntityReference, ...]:  # noqa: D102
        return tuple(path_element.join_on_entity for path_element in self.path_elements)

    @staticmethod
    def from_single_element(
        semantic_model_reference: SemanticModelReference, join_on_entity: EntityReference
    ) -> SemanticModelJoinPath:
        """Build SemanticModelJoinPath with just one join path element."""
        return SemanticModelJoinPath(
            path_elements=(
                SemanticModelJoinPathElement(
                    semantic_model_reference=semantic_model_reference,
                    join_on_entity=join_on_entity,
                ),
            )
        )


@dataclass(frozen=True)
class MetricSubqueryJoinPathElement:
    """Describes joining a metric subquery by the given entity."""

    metric_reference: MetricReference
    join_on_entity: EntityReference


@dataclass(frozen=True)
class MetricSubqueryJoinPath:
    """Describes how to join to a metric subquery.

    Starts with semantic model join path, if exists. Always ends with metric subquery join path.
    """

    metric_subquery_join_path_element: MetricSubqueryJoinPathElement
    semantic_model_join_path: Optional[SemanticModelJoinPath] = None

    @property
    def entity_links(self) -> Tuple[EntityReference, ...]:  # noqa: D102
        return (self.semantic_model_join_path.entity_links if self.semantic_model_join_path else ()) + (
            self.metric_subquery_join_path_element.join_on_entity,
        )

    @property
    def last_semantic_model_reference(self) -> Optional[SemanticModelReference]:
        """The last semantic model that would be joined in this path (if exists) before joining to metric."""
        return self.semantic_model_join_path.last_semantic_model_reference if self.semantic_model_join_path else None
