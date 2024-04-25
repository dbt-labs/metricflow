from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import FrozenSet, Optional, Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols.dimension import DimensionType
from dbt_semantic_interfaces.references import DimensionReference, MetricReference, SemanticModelReference
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.specs.spec_classes import EntityReference


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


@dataclass(frozen=True)
class SemanticModelJoinPathElement:
    """Describes joining a semantic model by the given entity."""

    semantic_model_reference: SemanticModelReference
    join_on_entity: EntityReference


@dataclass(frozen=True)
class LinkableDimension:
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


@dataclass(frozen=True)
class LinkableEntity:
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


@dataclass(frozen=True)
class LinkableMetric:
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
