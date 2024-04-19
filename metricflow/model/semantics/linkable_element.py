from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import FrozenSet, Optional, Tuple

from dbt_semantic_interfaces.references import DimensionReference, MetricReference, SemanticModelReference
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.specs.specs import EntityReference


class LinkableElementProperty(Enum):
    """The properties associated with a valid linkable element.

    Local means an element that is defined within the same semantic model as the measure. This definition is used
    throughout the related classes.
    """

    # A local element as per above definition.
    LOCAL = "local"
    # A local dimension that is prefixed with a local primary entity.
    LOCAL_LINKED = "local_linked"
    # An element that was joined to the measure semantic model by an entity.
    JOINED = "joined"
    # An element that was joined to the measure semantic model by joining multiple semantic models.
    MULTI_HOP = "multi_hop"
    # A time dimension that is a version of a time dimension in a semantic model, but at a different granularity.
    DERIVED_TIME_GRANULARITY = "derived_time_granularity"
    # Refers to an entity, not a dimension.
    ENTITY = "entity"
    # See metric_time in DataSet
    METRIC_TIME = "metric_time"
    # Refers to a metric, not a dimension.
    METRIC = "metric"

    @staticmethod
    def all_properties() -> FrozenSet[LinkableElementProperty]:  # noqa: D102
        return frozenset(
            {
                LinkableElementProperty.LOCAL,
                LinkableElementProperty.LOCAL_LINKED,
                LinkableElementProperty.JOINED,
                LinkableElementProperty.MULTI_HOP,
                LinkableElementProperty.DERIVED_TIME_GRANULARITY,
                LinkableElementProperty.METRIC_TIME,
                LinkableElementProperty.METRIC,
            }
        )


@dataclass(frozen=True)
class ElementPathKey:
    """A key that can uniquely identify an element and the joins used to realize the element."""

    element_name: str
    entity_links: Tuple[EntityReference, ...]
    time_granularity: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None


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
    entity_links: Tuple[EntityReference, ...]
    join_path: Tuple[SemanticModelJoinPathElement, ...]
    properties: FrozenSet[LinkableElementProperty]
    time_granularity: Optional[TimeGranularity]
    date_part: Optional[DatePart]

    @property
    def path_key(self) -> ElementPathKey:  # noqa: D102
        return ElementPathKey(
            element_name=self.element_name,
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
        return ElementPathKey(element_name=self.element_name, entity_links=self.entity_links)

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
        return ElementPathKey(element_name=self.element_name, entity_links=self.entity_links)

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
