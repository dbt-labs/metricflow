from __future__ import annotations

import collections
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from typing import FrozenSet, Iterable, Optional, Sequence, Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols.dimension import DimensionType
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    GroupByMetricReference,
    MetricReference,
    SemanticModelReference,
)
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from typing_extensions import override

from metricflow_semantics.assert_one_arg import assert_exactly_one_arg_set
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantic_model_derivation import SemanticModelDerivation
from metricflow_semantics.time.granularity import ExpandedTimeGranularity
from metricflow_semantics.workarounds.reference import sorted_semantic_model_references

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
    time_granularity: Optional[ExpandedTimeGranularity] = None
    date_part: Optional[DatePart] = None
    metric_subquery_entity_links: Tuple[EntityReference, ...] = ()

    def __post_init__(self) -> None:
        """Asserts all requirements associated with the element_type are met."""
        element_type = self.element_type
        if element_type is LinkableElementType.TIME_DIMENSION:
            assert (
                self.time_granularity
            ), "Time granularity must be specified for all ElementPathKeys associated with time dimensions!"
        elif (
            element_type is LinkableElementType.DIMENSION
            or element_type is LinkableElementType.ENTITY
            or element_type is LinkableElementType.METRIC
        ):
            pass
        else:
            assert_values_exhausted(element_type)

        assert len(set(self.entity_links)) == len(
            self.entity_links
        ), f"Duplicate found in `entity_links`: {self.entity_links}."


@dataclass(frozen=True)
class SemanticModelJoinPathElement(SerializableDataclass):
    """Describes joining a semantic model by the given entity."""

    semantic_model_reference: SemanticModelReference
    join_on_entity: EntityReference


@dataclass(frozen=True)
class LinkableElement(SemanticModelDerivation, SerializableDataclass, ABC):
    """An entity / dimension that may have been joined by entities."""

    properties: Tuple[LinkableElementProperty, ...]

    def __post_init__(self) -> None:  # noqa: D105
        if len(self.property_set) != len(self.properties):
            duplicate_properties = [item for item, count in collections.Counter(self.properties).items() if count > 1]
            assert False, f"Found duplicate properties {duplicate_properties} in {self.properties}"

        assert self.properties == tuple(sorted(self.properties)), f"Properties are not sorted: {self.properties}"

    @cached_property
    def property_set(self) -> FrozenSet[LinkableElementProperty]:  # noqa: D102
        return frozenset(self.properties)

    @property
    @abstractmethod
    def element_type(self) -> LinkableElementType:
        """The LinkableElementType describing what this instance represents."""
        raise NotImplementedError

    @property
    @abstractmethod
    def semantic_model_origin(self) -> SemanticModelReference:
        """The semantic model where this element was defined, if one exists.

        If no such model exists, the element will return the VIRTUAL_SEMANTIC_MODEL_REFERENCE, as it is
        either a virtual construct (e.g., metric_time) or a composite of semantic model inputs that could be used as
        a virtual semantic model at some point (e.g., metric queries requested as filter inputs).
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def as_union(self) -> LinkableElementUnion:
        """Return `self` in a union-type container for better serialization support."""
        raise NotImplementedError


@dataclass(frozen=True)
class LinkableDimension(LinkableElement, SerializableDataclass):
    """Describes how a dimension can be realized by joining based on entity links."""

    # The semantic model where this dimension was defined.
    defined_in_semantic_model: Optional[SemanticModelReference]
    element_name: str
    dimension_type: DimensionType
    entity_links: Tuple[EntityReference, ...]
    join_path: SemanticModelJoinPath
    time_granularity: Optional[ExpandedTimeGranularity]
    date_part: Optional[DatePart]

    @staticmethod
    def create(  # noqa: D102
        properties: Iterable[LinkableElementProperty],
        defined_in_semantic_model: Optional[SemanticModelReference],
        element_name: str,
        dimension_type: DimensionType,
        entity_links: Tuple[EntityReference, ...],
        join_path: SemanticModelJoinPath,
        time_granularity: Optional[ExpandedTimeGranularity],
        date_part: Optional[DatePart],
    ) -> LinkableDimension:
        return LinkableDimension(
            properties=tuple(sorted(set(properties))),
            defined_in_semantic_model=defined_in_semantic_model,
            element_name=element_name,
            dimension_type=dimension_type,
            entity_links=entity_links,
            join_path=join_path,
            time_granularity=time_granularity,
            date_part=date_part,
        )

    @property
    @override
    def element_type(self) -> LinkableElementType:
        return (
            LinkableElementType.DIMENSION
            if self.dimension_type is DimensionType.CATEGORICAL
            else LinkableElementType.TIME_DIMENSION
        )

    @property
    def path_key(self) -> ElementPathKey:  # noqa: D102
        return ElementPathKey(
            element_name=self.element_name,
            element_type=self.element_type,
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
        if self.defined_in_semantic_model:
            semantic_model_references.add(self.defined_in_semantic_model)
        semantic_model_references.update(self.join_path.derived_from_semantic_models)

        return sorted_semantic_model_references(semantic_model_references)

    @property
    @override
    def semantic_model_origin(self) -> SemanticModelReference:
        """Returns the semantic model reference pointing to the model where the dimension is defined.

        For virtual dimensions, such as metric_time, where there is no semantic model definition we return the
        virtual semantic model reference.
        """
        return (
            self.defined_in_semantic_model
            if self.defined_in_semantic_model
            else SemanticModelDerivation.VIRTUAL_SEMANTIC_MODEL_REFERENCE
        )

    @property
    @override
    def as_union(self) -> LinkableElementUnion:
        return LinkableElementUnion(linkable_dimension=self)


@dataclass(frozen=True)
class LinkableEntity(LinkableElement, SerializableDataclass):
    """Describes how an entity can be realized by joining based on entity links."""

    # The semantic model where this entity was defined.
    defined_in_semantic_model: SemanticModelReference
    element_name: str
    entity_links: Tuple[EntityReference, ...]
    join_path: SemanticModelJoinPath

    @staticmethod
    def create(  # noqa: D102
        properties: Iterable[LinkableElementProperty],
        defined_in_semantic_model: SemanticModelReference,
        element_name: str,
        entity_links: Tuple[EntityReference, ...],
        join_path: SemanticModelJoinPath,
    ) -> LinkableEntity:
        return LinkableEntity(
            properties=tuple(sorted(set(properties))),
            defined_in_semantic_model=defined_in_semantic_model,
            element_name=element_name,
            entity_links=entity_links,
            join_path=join_path,
        )

    @property
    @override
    def element_type(self) -> LinkableElementType:
        return LinkableElementType.ENTITY

    @property
    def path_key(self) -> ElementPathKey:  # noqa: D102
        return ElementPathKey(
            element_name=self.element_name, element_type=self.element_type, entity_links=self.entity_links
        )

    @property
    def reference(self) -> EntityReference:  # noqa: D102
        return EntityReference(element_name=self.element_name)

    @property
    @override
    def derived_from_semantic_models(self) -> Sequence[SemanticModelReference]:
        semantic_model_references = {self.defined_in_semantic_model}
        semantic_model_references.update(self.join_path.derived_from_semantic_models)
        return sorted_semantic_model_references(semantic_model_references)

    @property
    @override
    def semantic_model_origin(self) -> SemanticModelReference:
        return self.defined_in_semantic_model

    @property
    @override
    def as_union(self) -> LinkableElementUnion:
        return LinkableElementUnion(linkable_entity=self)


@dataclass(frozen=True)
class LinkableMetric(LinkableElement, SerializableDataclass):
    """Describes how a metric can be realized by joining based on entity links."""

    join_path: SemanticModelToMetricSubqueryJoinPath

    @staticmethod
    def create(  # noqa: D102
        properties: Iterable[LinkableElementProperty], join_path: SemanticModelToMetricSubqueryJoinPath
    ) -> LinkableMetric:
        return LinkableMetric(
            properties=tuple(sorted(set(properties))),
            join_path=join_path,
        )

    def __post_init__(self) -> None:
        """Ensure expected LinkableElementProperties have been set.

        LinkableMetrics always require a join to a metric subquery.
        """
        super().__post_init__()
        assert {LinkableElementProperty.METRIC, LinkableElementProperty.JOINED}.issubset(self.property_set)

    @property
    @override
    def element_type(self) -> LinkableElementType:
        return LinkableElementType.METRIC

    @property
    def element_name(self) -> str:  # noqa: D102
        return self.reference.element_name

    @property
    def path_key(self) -> ElementPathKey:  # noqa: D102
        return ElementPathKey(
            element_name=self.element_name,
            element_type=self.element_type,
            entity_links=self.join_path.entity_links,
            metric_subquery_entity_links=self.metric_subquery_entity_links,
        )

    @property
    def metric_reference(self) -> MetricReference:  # noqa: D102
        return self.join_path.metric_subquery_join_path_element.metric_reference

    @property
    def reference(self) -> GroupByMetricReference:  # noqa: D102
        return GroupByMetricReference(self.metric_reference.element_name)

    @property
    @override
    def derived_from_semantic_models(self) -> Sequence[SemanticModelReference]:
        """Semantic models needed to build and join to this LinkableMetric.

        Includes semantic models used in the join paths for both the inner and outer queries (if applicable),
        plus the semantic models the metric's measure(s) originated from.
        """
        semantic_model_references = set(self.join_path.metric_subquery_join_path_element.derived_from_semantic_models)
        if self.join_path.semantic_model_join_path:
            semantic_model_references.update(self.join_path.semantic_model_join_path.derived_from_semantic_models)
        if self.metric_to_entity_join_path:
            semantic_model_references.update(self.metric_to_entity_join_path.derived_from_semantic_models)

        return sorted_semantic_model_references(semantic_model_references)

    @property
    @override
    def semantic_model_origin(self) -> SemanticModelReference:
        """Returns the virtual semantic model reference, as metrics are not defined in semantic models.

        Metrics may be used as virtual source nodes for metrics as dimensions use cases, and we represent that here.
        """
        return SemanticModelDerivation.VIRTUAL_SEMANTIC_MODEL_REFERENCE

    @property
    def metric_to_entity_join_path(self) -> Optional[SemanticModelJoinPath]:
        """Join path used in metric subquery to join entity to metric, if needed."""
        return self.join_path.metric_subquery_join_path_element.metric_to_entity_join_path

    @property
    def metric_subquery_entity_links(self) -> Tuple[EntityReference, ...]:
        """Entity links used to join the metric to the entity it's grouped by in the metric subquery.

        Includes the `join_on_entity`, which will always be the last entity link.
        """
        return self.join_path.metric_subquery_entity_links

    @property
    @override
    def as_union(self) -> LinkableElementUnion:
        return LinkableElementUnion(linkable_metric=self)


@dataclass(frozen=True)
class LinkableElementUnion(SerializableDataclass):
    """A union type to use in classes that require a concrete implementation for serialization."""

    linkable_dimension: Optional[LinkableDimension] = None
    linkable_entity: Optional[LinkableEntity] = None
    linkable_metric: Optional[LinkableMetric] = None

    def __post_init__(self) -> None:  # noqa: D105
        assert_exactly_one_arg_set(
            linkable_dimension=self.linkable_dimension,
            linkable_entity=self.linkable_entity,
            linkable_metric=self.linkable_metric,
        )

    @property
    def linkable_element(self) -> LinkableElement:  # noqa: D102
        if self.linkable_dimension is not None:
            return self.linkable_dimension
        elif self.linkable_entity is not None:
            return self.linkable_entity
        elif self.linkable_metric is not None:
            return self.linkable_metric

        assert False, "All fields are None - this should have been caught in object initialization."


@dataclass(frozen=True)
class SemanticModelJoinPath(SemanticModelDerivation, SerializableDataclass):
    """Describes a series of joins between the measure semantic model, and other semantic models by entity.

    For example:

    (measure_source JOIN dimension_source0 ON entity A) JOIN dimension_source1 ON entity B

    would be represented by 2 path elements [(semantic_model0, A), (dimension_source1, B)]
    """

    left_semantic_model_reference: SemanticModelReference
    path_elements: Tuple[SemanticModelJoinPathElement, ...] = ()

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
        left_semantic_model_reference: SemanticModelReference,
        right_semantic_model_reference: SemanticModelReference,
        join_on_entity: EntityReference,
    ) -> SemanticModelJoinPath:
        """Build SemanticModelJoinPath with just one join path element."""
        return SemanticModelJoinPath(
            left_semantic_model_reference=left_semantic_model_reference,
            path_elements=(
                SemanticModelJoinPathElement(
                    semantic_model_reference=right_semantic_model_reference,
                    join_on_entity=join_on_entity,
                ),
            ),
        )

    @property
    def derived_from_semantic_models(self) -> Sequence[SemanticModelReference]:
        """Unique semantic models used in this join path."""
        semantic_model_references = set()
        semantic_model_references.add(self.left_semantic_model_reference)
        for path_element in self.path_elements:
            semantic_model_references.add(path_element.semantic_model_reference)

        return sorted_semantic_model_references(semantic_model_references)


@dataclass(frozen=True)
class MetricSubqueryJoinPathElement(SerializableDataclass):
    """Describes joining from a semantic model to a metric subquery.

    Args:
        metric_reference: The metric that's aggregated in the subquery.
        derived_from_semantic_models: The semantic models that the measure's input metrics are defined in.
        join_on_entity: The entity that the metric is grouped by in the subquery. This will be updated in V2 to allow a list
            of entities & dimensions.
        entity_links: Sequence of entities joined to get from a metric source to the `join_on_entity`. Should not include
            the `join_on_entity`.
        metric_to_entity_join_path: Describes the join path used in the subquery to join the metric to the `join_on_entity`.
            Can be none if all required elements are defined in the same semantic model.
    """

    metric_reference: MetricReference
    derived_from_semantic_models: Tuple[SemanticModelReference, ...]
    join_on_entity: EntityReference
    entity_links: Tuple[EntityReference, ...]
    metric_to_entity_join_path: Optional[SemanticModelJoinPath] = None

    def __post_init__(self) -> None:  # noqa: D105
        assert (
            self.derived_from_semantic_models
        ), "There must be at least one semantic model from which the metric is derived."


@dataclass(frozen=True)
class SemanticModelToMetricSubqueryJoinPath(SerializableDataclass):
    """Describes how to join from a semantic model to a metric subquery.

    Starts with semantic model join path, if needed. Always ends with metric subquery join path.
    """

    metric_subquery_join_path_element: MetricSubqueryJoinPathElement
    semantic_model_join_path: SemanticModelJoinPath

    @property
    def entity_links(self) -> Tuple[EntityReference, ...]:  # noqa: D102
        return (self.semantic_model_join_path.entity_links if self.semantic_model_join_path else ()) + (
            self.metric_subquery_join_path_element.join_on_entity,
        )

    @property
    def metric_subquery_entity_links(self) -> Tuple[EntityReference, ...]:  # noqa: D102
        return self.metric_subquery_join_path_element.entity_links + (
            self.metric_subquery_join_path_element.join_on_entity,
        )
