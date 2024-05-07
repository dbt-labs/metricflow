"""These spec classes generally describe something that we want or have.

* Not too many comments here since they seem mostly self-explanatory.
* To see whether a spec matches something that already exists, there could be a method that allows you to match a spec
  to another spec or relevant object.
* The match() method will enable sub-classes (may require some restructuring) to use specs to request things like,
  metrics named "sales*".

TODO: Split this file into separate files.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from hashlib import sha1
from typing import Any, Generic, List, Optional, Sequence, Tuple, TypeVar, Union

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from dbt_semantic_interfaces.implementations.metric import PydanticMetricTimeWindow
from dbt_semantic_interfaces.naming.keywords import DUNDER, METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.protocols import MetricTimeWindow
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    LinkableElementReference,
    MeasureReference,
    MetricReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from typing_extensions import override

from metricflow_semantics.aggregation_properties import AggregationState
from metricflow_semantics.collection_helpers.dedupe import ordered_dedupe
from metricflow_semantics.collection_helpers.merger import Mergeable
from metricflow_semantics.model.semantics.linkable_element import (
    ElementPathKey,
    GroupByMetricReference,
    LinkableElement,
    LinkableElementType,
)
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameters
from metricflow_semantics.sql.sql_column_type import SqlColumnType
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.visitor import VisitorOutputT

logger = logging.getLogger(__name__)


def hash_items(items: Sequence[SqlColumnType]) -> str:
    """Produces a hash from a list of strings."""
    hash_builder = sha1()
    for item in items:
        hash_builder.update(str(item).encode("utf-8"))
    return hash_builder.hexdigest()


class InstanceSpecVisitor(Generic[VisitorOutputT], ABC):
    """Visitor for the InstanceSpec classes."""

    @abstractmethod
    def visit_measure_spec(self, measure_spec: MeasureSpec) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_dimension_spec(self, dimension_spec: DimensionSpec) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_time_dimension_spec(self, time_dimension_spec: TimeDimensionSpec) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_entity_spec(self, entity_spec: EntitySpec) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_group_by_metric_spec(self, group_by_metric_spec: GroupByMetricSpec) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_metric_spec(self, metric_spec: MetricSpec) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_metadata_spec(self, metadata_spec: MetadataSpec) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError


@dataclass(frozen=True)
class InstanceSpec(SerializableDataclass):
    """A specification for an instance of a metric definition object.

    An instance is different from the definition object in that it correlates to columns in the data flow and can be in
    different states. e.g. a time dimension at a different time granularity.

    This can't be a Protocol as base classes of Protocols need to be Protocols.
    """

    """Name of the dimension or entity in the semantic model."""
    element_name: str

    @staticmethod
    def merge(*specs: Sequence[InstanceSpec]) -> List[InstanceSpec]:
        """Merge all specs into a single list."""
        result: List[InstanceSpec] = []
        for spec in specs:
            result.extend(spec)
        return result

    @property
    def qualified_name(self) -> str:
        """Return the qualified name of this spec. e.g. "user_id__country"."""
        raise NotImplementedError()

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:
        """See Visitable."""
        raise NotImplementedError()


SelfTypeT = TypeVar("SelfTypeT", bound="LinkableInstanceSpec")


@dataclass(frozen=True)
class MetadataSpec(InstanceSpec):
    """A specification for a specification that is built during the dataflow plan and not defined in config."""

    element_name: str
    agg_type: Optional[AggregationType] = None

    @property
    def qualified_name(self) -> str:  # noqa: D102
        return f"{self.element_name}{DUNDER}{self.agg_type.value}" if self.agg_type else self.element_name

    @staticmethod
    def from_name(name: str, agg_type: Optional[AggregationType] = None) -> MetadataSpec:  # noqa: D102
        return MetadataSpec(element_name=name, agg_type=agg_type)

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_metadata_spec(self)


@dataclass(frozen=True)
class LinkableInstanceSpec(InstanceSpec, ABC):
    """Generally a dimension or entity that may be specified using entity links.

    For example, user_id__country -> LinkableElementSpec(element_name="country", entity_links=["user_id"]

    See InstanceSpec for the reason behind "type: ignore"
    """

    """A list representing the join path of entities to get to this element."""
    entity_links: Tuple[EntityReference, ...]

    @property
    @abstractmethod
    def without_first_entity_link(self: SelfTypeT) -> SelfTypeT:
        """e.g. user_id__device_id__platform -> device_id__platform."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def without_entity_links(self: SelfTypeT) -> SelfTypeT:
        """e.g. user_id__device_id__platform -> platform."""
        raise NotImplementedError()

    @staticmethod
    def merge_linkable_specs(*specs: Sequence[LinkableInstanceSpec]) -> List[LinkableInstanceSpec]:
        """Merge all specs into a single list."""
        result: List[LinkableInstanceSpec] = []
        for spec in specs:
            result.extend(spec)
        return result

    @property
    def qualified_name(self) -> str:
        """Return the qualified name of this spec. e.g. "user_id__country"."""
        return StructuredLinkableSpecName(
            entity_link_names=tuple(x.element_name for x in self.entity_links), element_name=self.element_name
        ).qualified_name

    @property
    @abstractmethod
    def reference(self) -> LinkableElementReference:
        """Return the LinkableElementReference associated with the spec instance."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def element_path_key(self) -> ElementPathKey:
        """Return the ElementPathKey representation of the LinkableInstanceSpec subtype."""
        raise NotImplementedError()


@dataclass(frozen=True)
class EntitySpec(LinkableInstanceSpec, SerializableDataclass):  # noqa: D101
    @property
    def without_first_entity_link(self) -> EntitySpec:  # noqa: D102
        assert len(self.entity_links) > 0, f"Spec does not have any entity links: {self}"
        return EntitySpec(element_name=self.element_name, entity_links=self.entity_links[1:])

    @property
    def without_entity_links(self) -> EntitySpec:  # noqa: D102
        return LinklessEntitySpec.from_element_name(self.element_name)

    @property
    def as_linkless_prefix(self) -> Tuple[EntityReference, ...]:
        """Creates tuple of linkless entities that could be included in the entity_links of another spec.

        eg as a prefix to a DimensionSpec's entity links to when a join is occurring via this entity
        """
        return (EntityReference(element_name=self.element_name),) + self.entity_links

    @staticmethod
    def from_name(name: str) -> EntitySpec:  # noqa: D102
        structured_name = StructuredLinkableSpecName.from_name(name)
        return EntitySpec(
            entity_links=tuple(EntityReference(idl) for idl in structured_name.entity_link_names),
            element_name=structured_name.element_name,
        )

    def __eq__(self, other: Any) -> bool:  # type: ignore[misc]  # noqa: D105
        if not isinstance(other, EntitySpec):
            return False
        return self.element_name == other.element_name and self.entity_links == other.entity_links

    def __hash__(self) -> int:  # noqa: D105
        return hash((self.element_name, self.entity_links))

    @property
    def reference(self) -> EntityReference:  # noqa: D102
        return EntityReference(element_name=self.element_name)

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_entity_spec(self)

    @property
    @override
    def element_path_key(self) -> ElementPathKey:
        return ElementPathKey(
            element_name=self.element_name, element_type=LinkableElementType.ENTITY, entity_links=self.entity_links
        )


@dataclass(frozen=True)
class LinklessEntitySpec(EntitySpec, SerializableDataclass):
    """Similar to EntitySpec, but requires that it doesn't have entity links."""

    @staticmethod
    def from_element_name(element_name: str) -> LinklessEntitySpec:  # noqa: D102
        return LinklessEntitySpec(element_name=element_name, entity_links=())

    def __post_init__(self) -> None:  # noqa: D105
        if len(self.entity_links) > 0:
            raise RuntimeError(f"{self.__class__.__name__} shouldn't have entity links. Got: {self}")

    def __eq__(self, other: Any) -> bool:  # type: ignore[misc]  # noqa: D105
        if not isinstance(other, EntitySpec):
            return False
        return self.element_name == other.element_name and self.entity_links == other.entity_links

    def __hash__(self) -> int:  # noqa: D105
        return hash((self.element_name, self.entity_links))

    @staticmethod
    def from_reference(entity_reference: EntityReference) -> LinklessEntitySpec:  # noqa: D102
        return LinklessEntitySpec(element_name=entity_reference.element_name, entity_links=())


@dataclass(frozen=True)
class DimensionSpec(LinkableInstanceSpec, SerializableDataclass):  # noqa: D101
    element_name: str
    entity_links: Tuple[EntityReference, ...]

    @property
    def without_first_entity_link(self) -> DimensionSpec:  # noqa: D102
        assert len(self.entity_links) > 0, f"Spec does not have any entity links: {self}"
        return DimensionSpec(element_name=self.element_name, entity_links=self.entity_links[1:])

    @property
    def without_entity_links(self) -> DimensionSpec:  # noqa: D102
        return DimensionSpec(element_name=self.element_name, entity_links=())

    @staticmethod
    def from_linkable(spec: LinkableInstanceSpec) -> DimensionSpec:  # noqa: D102
        return DimensionSpec(element_name=spec.element_name, entity_links=spec.entity_links)

    @staticmethod
    def from_name(name: str) -> DimensionSpec:
        """Construct from a name e.g. listing__ds__month."""
        parsed_name = StructuredLinkableSpecName.from_name(name)
        return DimensionSpec(
            entity_links=tuple([EntityReference(idl) for idl in parsed_name.entity_link_names]),
            element_name=parsed_name.element_name,
        )

    @property
    def reference(self) -> DimensionReference:  # noqa: D102
        return DimensionReference(element_name=self.element_name)

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_dimension_spec(self)

    @property
    @override
    def element_path_key(self) -> ElementPathKey:
        return ElementPathKey(
            element_name=self.element_name, element_type=LinkableElementType.DIMENSION, entity_links=self.entity_links
        )


class TimeDimensionSpecField(Enum):
    """Fields of the time dimension spec.

    The value corresponds to the name of the field in the dataclass. This should contain all fields, but implementation
    is pending.
    """

    TIME_GRANULARITY = "time_granularity"


class TimeDimensionSpecComparisonKey:
    """A key that can be used for comparing / grouping time dimension specs.

    Useful for assessing if two time dimension specs are equal while ignoring specific attributes. Keys must have the
    same set of excluded attributes to be valid for comparison.

    This is useful for ambiguous group-by-item resolution where we want to select a time dimension regardless of the
    grain.
    """

    def __init__(self, source_spec: TimeDimensionSpec, exclude_fields: Sequence[TimeDimensionSpecField]) -> None:
        """Initializer.

        Args:
            source_spec: The spec that this key is based on.
            exclude_fields: The fields to ignore when determining equality.
        """
        self._excluded_fields = frozenset(exclude_fields)
        self._source_spec = source_spec

        # This is a list of field values of TimeDimensionSpec that we should use for comparison.
        spec_field_values_for_comparison: List[
            Union[str, Tuple[EntityReference, ...], TimeGranularity, Optional[DatePart]]
        ] = [self._source_spec.element_name, self._source_spec.entity_links]

        if TimeDimensionSpecField.TIME_GRANULARITY not in self._excluded_fields:
            spec_field_values_for_comparison.append(self._source_spec.time_granularity)

        spec_field_values_for_comparison.append(self._source_spec.date_part)

        self._spec_field_values_for_comparison = tuple(spec_field_values_for_comparison)

    @property
    def source_spec(self) -> TimeDimensionSpec:  # noqa: D102
        return self._source_spec

    @override
    def __eq__(self, other: Any) -> bool:  # type: ignore[misc]
        if not isinstance(other, TimeDimensionSpecComparisonKey):
            return False

        if self._excluded_fields != other._excluded_fields:
            return False

        return self._spec_field_values_for_comparison == other._spec_field_values_for_comparison

    @override
    def __hash__(self) -> int:
        return hash((self._excluded_fields, self._spec_field_values_for_comparison))


DEFAULT_TIME_GRANULARITY = TimeGranularity.DAY


@dataclass(frozen=True)
class TimeDimensionSpec(DimensionSpec):  # noqa: D101
    time_granularity: TimeGranularity = DEFAULT_TIME_GRANULARITY
    date_part: Optional[DatePart] = None

    # Used for semi-additive joins. Some more thought is needed, but this may be useful in InstanceSpec.
    aggregation_state: Optional[AggregationState] = None

    @property
    def without_first_entity_link(self) -> TimeDimensionSpec:  # noqa: D102
        assert len(self.entity_links) > 0, f"Spec does not have any entity links: {self}"
        return TimeDimensionSpec(
            element_name=self.element_name,
            entity_links=self.entity_links[1:],
            time_granularity=self.time_granularity,
            date_part=self.date_part,
        )

    @property
    def without_entity_links(self) -> TimeDimensionSpec:  # noqa: D102
        return TimeDimensionSpec.from_name(self.element_name)

    @staticmethod
    def from_name(name: str) -> TimeDimensionSpec:  # noqa: D102
        structured_name = StructuredLinkableSpecName.from_name(name)
        return TimeDimensionSpec(
            entity_links=tuple(EntityReference(idl) for idl in structured_name.entity_link_names),
            element_name=structured_name.element_name,
            time_granularity=structured_name.time_granularity or DEFAULT_TIME_GRANULARITY,
        )

    @property
    def reference(self) -> TimeDimensionReference:  # noqa: D102
        return TimeDimensionReference(element_name=self.element_name)

    @property
    def dimension_reference(self) -> DimensionReference:  # noqa: D102
        return DimensionReference(element_name=self.element_name)

    @property
    def qualified_name(self) -> str:  # noqa: D102
        return StructuredLinkableSpecName(
            entity_link_names=tuple(x.element_name for x in self.entity_links),
            element_name=self.element_name,
            time_granularity=self.time_granularity,
            date_part=self.date_part,
        ).qualified_name

    @property
    @override
    def element_path_key(self) -> ElementPathKey:
        return ElementPathKey(
            element_name=self.element_name,
            element_type=LinkableElementType.TIME_DIMENSION,
            entity_links=self.entity_links,
            time_granularity=self.time_granularity,
            date_part=self.date_part,
        )

    @staticmethod
    def from_reference(reference: TimeDimensionReference) -> TimeDimensionSpec:
        """Initialize from a time dimension reference instance."""
        return TimeDimensionSpec(entity_links=(), element_name=reference.element_name)

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_time_dimension_spec(self)

    def with_grain(self, time_granularity: TimeGranularity) -> TimeDimensionSpec:  # noqa: D102
        return TimeDimensionSpec(
            element_name=self.element_name,
            entity_links=self.entity_links,
            time_granularity=time_granularity,
            date_part=self.date_part,
            aggregation_state=self.aggregation_state,
        )

    def with_aggregation_state(self, aggregation_state: AggregationState) -> TimeDimensionSpec:  # noqa: D102
        return TimeDimensionSpec(
            element_name=self.element_name,
            entity_links=self.entity_links,
            time_granularity=self.time_granularity,
            date_part=self.date_part,
            aggregation_state=aggregation_state,
        )

    def comparison_key(self, exclude_fields: Sequence[TimeDimensionSpecField] = ()) -> TimeDimensionSpecComparisonKey:
        """See TimeDimensionComparisonKey."""
        return TimeDimensionSpecComparisonKey(
            source_spec=self,
            exclude_fields=exclude_fields,
        )

    @staticmethod
    def generate_possible_specs_for_time_dimension(
        time_dimension_reference: TimeDimensionReference, entity_links: Tuple[EntityReference, ...]
    ) -> List[TimeDimensionSpec]:
        """Generate a list of time dimension specs with all combinations of granularity & date part."""
        time_dimension_specs: List[TimeDimensionSpec] = []
        for time_granularity in TimeGranularity:
            time_dimension_specs.append(
                TimeDimensionSpec(
                    element_name=time_dimension_reference.element_name,
                    entity_links=entity_links,
                    time_granularity=time_granularity,
                    date_part=None,
                )
            )
        for date_part in DatePart:
            for time_granularity in date_part.compatible_granularities:
                time_dimension_specs.append(
                    TimeDimensionSpec(
                        element_name=time_dimension_reference.element_name,
                        entity_links=entity_links,
                        time_granularity=time_granularity,
                        date_part=date_part,
                    )
                )
        return time_dimension_specs

    @property
    def is_metric_time(self) -> bool:  # noqa: D102
        return self.element_name == METRIC_TIME_ELEMENT_NAME


@dataclass(frozen=True)
class NonAdditiveDimensionSpec(SerializableDataclass):
    """Spec representing non-additive dimension parameters for use within a MeasureSpec.

    This is sourced from the NonAdditiveDimensionParameters model object, which provides the parsed parameter set,
    while the spec contains the information needed for dataflow plan operations
    """

    name: str
    window_choice: AggregationType
    window_groupings: Tuple[str, ...] = ()

    @property
    def bucket_hash(self) -> str:
        """Returns the hash value used for grouping equivalent params."""
        values = [self.window_choice.name, self.name]
        values.extend(sorted(self.window_groupings))
        return hash_items(values)

    @property
    def linkable_specs(self) -> Sequence[LinkableInstanceSpec]:  # noqa: D102
        return (TimeDimensionSpec.from_name(self.name),) + tuple(
            LinklessEntitySpec.from_element_name(entity_name) for entity_name in self.window_groupings
        )

    def __eq__(self, other: Any) -> bool:  # type: ignore[misc]  # noqa: D105
        if not isinstance(other, NonAdditiveDimensionSpec):
            return False
        return self.bucket_hash == other.bucket_hash


@dataclass(frozen=True)
class MeasureSpec(InstanceSpec):  # noqa: D101
    element_name: str
    non_additive_dimension_spec: Optional[NonAdditiveDimensionSpec] = None
    fill_nulls_with: Optional[int] = None

    @staticmethod
    def from_name(name: str) -> MeasureSpec:
        """Construct from a name e.g. listing__ds__month."""
        return MeasureSpec(element_name=name)

    @staticmethod
    def from_reference(reference: MeasureReference) -> MeasureSpec:
        """Initialize from a measure reference instance."""
        return MeasureSpec(element_name=reference.element_name)

    @property
    def qualified_name(self) -> str:  # noqa: D102
        return self.element_name

    @property
    def reference(self) -> MeasureReference:  # noqa: D102
        return MeasureReference(element_name=self.element_name)

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_measure_spec(self)


@dataclass(frozen=True)
class MetricSpec(InstanceSpec):  # noqa: D101
    # Time-over-time could go here
    element_name: str
    filter_specs: Tuple[WhereFilterSpec, ...] = ()
    alias: Optional[str] = None
    offset_window: Optional[PydanticMetricTimeWindow] = None
    offset_to_grain: Optional[TimeGranularity] = None

    @staticmethod
    def from_element_name(element_name: str) -> MetricSpec:  # noqa: D102
        return MetricSpec(element_name=element_name)

    @property
    def qualified_name(self) -> str:  # noqa: D102
        return self.element_name

    @staticmethod
    def from_reference(reference: MetricReference) -> MetricSpec:
        """Initialize from a metric reference instance."""
        return MetricSpec(element_name=reference.element_name)

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_metric_spec(self)

    @property
    def reference(self) -> MetricReference:
        """Return the reference object that is used for referencing the associated metric in the manifest."""
        return MetricReference(element_name=self.element_name)

    @property
    def has_time_offset(self) -> bool:  # noqa: D102
        return bool(self.offset_window or self.offset_to_grain)

    def without_offset(self) -> MetricSpec:
        """Represents the metric spec with any time offsets removed."""
        return MetricSpec(element_name=self.element_name, filter_specs=self.filter_specs, alias=self.alias)


@dataclass(frozen=True)
class CumulativeMeasureDescription:
    """If a measure is a part of a cumulative metric, this represents the associated parameters."""

    cumulative_window: Optional[MetricTimeWindow]
    cumulative_grain_to_date: Optional[TimeGranularity]


@dataclass(frozen=True)
class MetricInputMeasureSpec(SerializableDataclass):
    """The spec for a measure defined as a base metric input.

    This is necessary because the MeasureSpec is used as a key linking the measures used in the query
    to the measures defined in the semantic models. Adding metric-specific information, like constraints,
    causes lookups connecting query -> semantic model to fail in strange ways. This spec, then, provides
    both the key (in the form of a MeasureSpec) along with whatever measure-specific attributes
    a user might specify in a metric definition or query accessing the metric itself.
    """

    measure_spec: MeasureSpec
    fill_nulls_with: Optional[int] = None
    offset_window: Optional[MetricTimeWindow] = None
    offset_to_grain: Optional[TimeGranularity] = None
    cumulative_description: Optional[CumulativeMeasureDescription] = None
    filter_specs: Tuple[WhereFilterSpec, ...] = ()
    alias: Optional[str] = None
    before_aggregation_time_spine_join_description: Optional[JoinToTimeSpineDescription] = None
    after_aggregation_time_spine_join_description: Optional[JoinToTimeSpineDescription] = None

    @property
    def post_aggregation_spec(self) -> MeasureSpec:
        """Return a MeasureSpec instance representing the post-aggregation spec state for the underlying measure."""
        if self.alias:
            return MeasureSpec(
                element_name=self.alias,
                non_additive_dimension_spec=self.measure_spec.non_additive_dimension_spec,
                fill_nulls_with=self.fill_nulls_with,
            )
        else:
            return self.measure_spec


@dataclass(frozen=True)
class OrderBySpec(SerializableDataclass):  # noqa: D101
    instance_spec: InstanceSpec
    descending: bool


@dataclass(frozen=True)
class WhereFilterSpec(Mergeable, SerializableDataclass):
    """Similar to the WhereFilter, but with the where_sql_template rendered and used elements extracted.

    For example:

    WhereFilter(where_sql_template="{{ Dimension('listing__country') }} == 'US'"))

    ->

    WhereFilterSpec(
        where_sql="listing__country == 'US'",
        bind_parameters: SqlBindParameters(),
        linkable_specs: (
            DimensionSpec(
                element_name='country',
                entity_links=('listing',),
        ),
        linkable_elements: (
            LinkableDimension(
                semantic_model_origin=SemanticModelReference(semantic_model_name='listings_latest')
                element_name='country',
                ...
            )
        )
    )
    """

    # Debating whether where_sql / bind_parameters belongs here. where_sql may become dialect specific if we introduce
    # quoted identifiers later.
    where_sql: str
    bind_parameters: SqlBindParameters
    linkable_specs: Tuple[LinkableInstanceSpec, ...]
    linkable_elements: Tuple[LinkableElement, ...]

    def merge(self, other: WhereFilterSpec) -> WhereFilterSpec:  # noqa: D102
        if self == WhereFilterSpec.empty_instance():
            return other

        if other == WhereFilterSpec.empty_instance():
            return self

        if self == other:
            return self

        return WhereFilterSpec(
            where_sql=f"({self.where_sql}) AND ({other.where_sql})",
            bind_parameters=self.bind_parameters.combine(other.bind_parameters),
            linkable_specs=ordered_dedupe(self.linkable_specs, other.linkable_specs),
            linkable_elements=ordered_dedupe(self.linkable_elements, other.linkable_elements),
        )

    @classmethod
    @override
    def empty_instance(cls) -> WhereFilterSpec:
        # Need to revisit making WhereFilterSpec Mergeable as it's current not a collection, and it's odd to return this
        # no-op filter. Use cases would need to check whether a WhereSpec is a no-op before rendering it to avoid an
        # un-necessary WHERE clause. Making WhereFilterSpec map to a WhereFilterIntersection would make this more in
        # line with other cases of Mergeable.
        return WhereFilterSpec(
            where_sql="TRUE",
            bind_parameters=SqlBindParameters(),
            linkable_specs=(),
            linkable_elements=(),
        )


@dataclass(frozen=True)
class ConstantPropertySpec(SerializableDataclass):
    """Includes the specs that are joined for conversion metric's constant properties."""

    base_spec: LinkableInstanceSpec
    conversion_spec: LinkableInstanceSpec


@dataclass(frozen=True)
class JoinToTimeSpineDescription:
    """Describes how a time spine join should be performed."""

    join_type: SqlJoinType
    offset_window: Optional[MetricTimeWindow]
    offset_to_grain: Optional[TimeGranularity]


@dataclass(frozen=True)
class GroupByMetricSpec(LinkableInstanceSpec, SerializableDataclass):
    """Metric used in group by or where filter.

    Args:
        element_name: Name of the metric being joined.
        entity_links: Sequence of entities joined to join the metric subquery to the outer query. Last entity is the one
            joining the subquery to the outer query.
        metric_subquery_entity_links: Sequence of entities used in the metric subquery to join the metric to the entity.
    """

    metric_subquery_entity_links: Tuple[EntityReference, ...]

    def __post_init__(self) -> None:
        """The inner query and outer query entity paths must end with the same entity (that's what they join on).

        If no entity links, it's because we're already in the final joined node (no links left).
        """
        assert (
            len(self.metric_subquery_entity_links) > 0
        ), "GroupByMetricSpec must have at least one metric_subquery_entity_link."
        if self.entity_links:
            assert (
                self.metric_subquery_entity_links[-1] == self.entity_links[-1]
            ), "Inner and outer query must have the same last entity link in order to join on that link."

    @property
    def without_first_entity_link(self) -> GroupByMetricSpec:  # noqa: D102
        assert len(self.entity_links) > 0, f"Spec does not have any entity links: {self}"
        return GroupByMetricSpec(
            element_name=self.element_name,
            entity_links=self.entity_links[1:],
            metric_subquery_entity_links=self.metric_subquery_entity_links,
        )

    @property
    def without_entity_links(self) -> GroupByMetricSpec:  # noqa: D102
        return GroupByMetricSpec(
            element_name=self.element_name,
            entity_links=(),
            metric_subquery_entity_links=self.metric_subquery_entity_links,
        )

    @property
    def last_entity_link(self) -> EntityReference:  # noqa: D102
        assert len(self.entity_links) > 0, f"Spec does not have any entity links: {self}"
        return self.entity_links[-1]

    @property
    def metric_subquery_entity_spec(self) -> EntitySpec:
        """Spec for the entity that the metric will be grouped by in the metric subquery."""
        assert (
            len(self.metric_subquery_entity_links) > 0
        ), "GroupByMetricSpec must have at least one metric_subquery_entity_link."
        return EntitySpec(
            element_name=self.metric_subquery_entity_links[-1].element_name,
            entity_links=self.metric_subquery_entity_links[:-1],
        )

    @property
    def qualified_name(self) -> str:
        """Element name prefixed with entity links.

        If same entity links are used in inner & outer query, use standard qualified name (country__bookings).
        Else, specify both sets of entity links (listing__country__user__country__bookings).
        """
        if self.entity_links == self.metric_subquery_entity_links:
            entity_links = self.entity_links
        else:
            entity_links = self.entity_links + self.metric_subquery_entity_links

        return StructuredLinkableSpecName(
            entity_link_names=tuple(entity_link.element_name for entity_link in entity_links),
            element_name=self.element_name,
        ).qualified_name

    def __eq__(self, other: Any) -> bool:  # type: ignore[misc] # noqa: D105
        if not isinstance(other, GroupByMetricSpec):
            return False
        return self.element_name == other.element_name and self.entity_links == other.entity_links

    def __hash__(self) -> int:  # noqa: D105
        return hash((self.element_name, self.entity_links, self.metric_subquery_entity_links))

    @property
    def reference(self) -> GroupByMetricReference:  # noqa: D102
        return GroupByMetricReference(element_name=self.element_name)

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_group_by_metric_spec(self)

    @property
    @override
    def element_path_key(self) -> ElementPathKey:
        return ElementPathKey(
            element_name=self.element_name, element_type=LinkableElementType.METRIC, entity_links=self.entity_links
        )
