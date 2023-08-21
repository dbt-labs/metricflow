"""These spec classes generally describe something that we want or have.

* Not too many comments here since they seem mostly self-explanatory.
* To see whether a spec matches something that already exists, there could be a method that allows you to match a spec
  to another spec or relevant object.
* The match() method will enable sub-classes (may require some restructuring) to use specs to request things like,
  metrics named "sales*".
"""

from __future__ import annotations

import itertools
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from hashlib import sha1
from typing import Any, Generic, List, Optional, Sequence, Tuple, TypeVar

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from dbt_semantic_interfaces.implementations.metric import PydanticMetricTimeWindow
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    MeasureReference,
    MetricReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.aggregation_properties import AggregationState
from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql.sql_column_type import SqlColumnType
from metricflow.time.date_part import DatePart
from metricflow.visitor import VisitorOutputT


def hash_items(items: Sequence[SqlColumnType]) -> str:
    """Produces a hash from a list of strings."""
    hash_builder = sha1()
    for item in items:
        hash_builder.update(str(item).encode("utf-8"))
    return hash_builder.hexdigest()


class InstanceSpecVisitor(Generic[VisitorOutputT], ABC):
    """Visitor for the InstanceSpec classes."""

    @abstractmethod
    def visit_measure_spec(self, measure_spec: MeasureSpec) -> VisitorOutputT:  # noqa: D
        raise NotImplementedError

    @abstractmethod
    def visit_dimension_spec(self, dimension_spec: DimensionSpec) -> VisitorOutputT:  # noqa: D
        raise NotImplementedError

    @abstractmethod
    def visit_time_dimension_spec(self, time_dimension_spec: TimeDimensionSpec) -> VisitorOutputT:  # noqa: D
        raise NotImplementedError

    @abstractmethod
    def visit_entity_spec(self, entity_spec: EntitySpec) -> VisitorOutputT:  # noqa: D
        raise NotImplementedError

    @abstractmethod
    def visit_metric_spec(self, metric_spec: MetricSpec) -> VisitorOutputT:  # noqa: D
        raise NotImplementedError

    @abstractmethod
    def visit_metadata_spec(self, metadata_spec: MetadataSpec) -> VisitorOutputT:  # noqa: D
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

    @property
    def qualified_name(self) -> str:  # noqa: D
        return self.element_name

    @staticmethod
    def from_name(name: str) -> MetadataSpec:  # noqa: D
        return MetadataSpec(element_name=name)

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
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
    def without_first_entity_link(self: SelfTypeT) -> SelfTypeT:
        """e.g. user_id__device_id__platform -> device_id__platform."""
        raise NotImplementedError()

    @property
    def without_entity_links(self: SelfTypeT) -> SelfTypeT:  # noqa: D
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
    def as_linkable_spec_set(self) -> LinkableSpecSet:  # noqa: D
        raise NotImplementedError


@dataclass(frozen=True)
class EntitySpec(LinkableInstanceSpec, SerializableDataclass):  # noqa: D
    @property
    def without_first_entity_link(self) -> EntitySpec:  # noqa: D
        assert len(self.entity_links) > 0, f"Spec does not have any entity links: {self}"
        return EntitySpec(element_name=self.element_name, entity_links=self.entity_links[1:])

    @property
    def without_entity_links(self) -> EntitySpec:  # noqa: D
        return LinklessEntitySpec.from_element_name(self.element_name)

    @property
    def as_linkless_prefix(self) -> Tuple[EntityReference, ...]:
        """Creates tuple of linkless entities that could be included in the entity_links of another spec.

        eg as a prefix to a DimensionSpec's entity links to when a join is occurring via this entity
        """
        return (EntityReference(element_name=self.element_name),) + self.entity_links

    @staticmethod
    def from_name(name: str) -> EntitySpec:  # noqa: D
        structured_name = StructuredLinkableSpecName.from_name(name)
        return EntitySpec(
            entity_links=tuple(EntityReference(idl) for idl in structured_name.entity_link_names),
            element_name=structured_name.element_name,
        )

    def __eq__(self, other: Any) -> bool:  # type: ignore[misc] # noqa: D
        if not isinstance(other, EntitySpec):
            return False
        return self.element_name == other.element_name and self.entity_links == other.entity_links

    def __hash__(self) -> int:  # noqa: D
        return hash((self.element_name, self.entity_links))

    @property
    def reference(self) -> EntityReference:  # noqa: D
        return EntityReference(element_name=self.element_name)

    @property
    def as_linkable_spec_set(self) -> LinkableSpecSet:  # noqa: D
        return LinkableSpecSet(entity_specs=(self,))

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_entity_spec(self)


@dataclass(frozen=True)
class LinklessEntitySpec(EntitySpec, SerializableDataclass):
    """Similar to EntitySpec, but requires that it doesn't have entity links."""

    @staticmethod
    def from_element_name(element_name: str) -> LinklessEntitySpec:  # noqa: D
        return LinklessEntitySpec(element_name=element_name, entity_links=())

    def __post_init__(self) -> None:  # noqa: D
        if len(self.entity_links) > 0:
            raise RuntimeError(f"{self.__class__.__name__} shouldn't have entity links. Got: {self}")

    def __eq__(self, other: Any) -> bool:  # type: ignore[misc] # noqa: D
        if not isinstance(other, EntitySpec):
            return False
        return self.element_name == other.element_name and self.entity_links == other.entity_links

    def __hash__(self) -> int:  # noqa: D
        return hash((self.element_name, self.entity_links))

    @staticmethod
    def from_reference(entity_reference: EntityReference) -> LinklessEntitySpec:  # noqa: D
        return LinklessEntitySpec(element_name=entity_reference.element_name, entity_links=())


@dataclass(frozen=True)
class DimensionSpec(LinkableInstanceSpec, SerializableDataclass):  # noqa: D
    element_name: str
    entity_links: Tuple[EntityReference, ...]

    @property
    def without_first_entity_link(self) -> DimensionSpec:  # noqa: D
        assert len(self.entity_links) > 0, f"Spec does not have any entity links: {self}"
        return DimensionSpec(element_name=self.element_name, entity_links=self.entity_links[1:])

    @property
    def without_entity_links(self) -> DimensionSpec:  # noqa: D
        return DimensionSpec(element_name=self.element_name, entity_links=())

    @staticmethod
    def from_linkable(spec: LinkableInstanceSpec) -> DimensionSpec:  # noqa: D
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
    def reference(self) -> DimensionReference:  # noqa: D
        return DimensionReference(element_name=self.element_name)

    @property
    def as_linkable_spec_set(self) -> LinkableSpecSet:  # noqa: D
        return LinkableSpecSet(dimension_specs=(self,))

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_dimension_spec(self)


DEFAULT_TIME_GRANULARITY = TimeGranularity.DAY


@dataclass(frozen=True)
class TimeDimensionSpec(DimensionSpec):  # noqa: D
    time_granularity: TimeGranularity = DEFAULT_TIME_GRANULARITY
    date_part: Optional[DatePart] = None

    # Used for semi-additive joins. Some more thought is needed, but this may be useful in InstanceSpec.
    aggregation_state: Optional[AggregationState] = None

    @property
    def without_first_entity_link(self) -> TimeDimensionSpec:  # noqa: D
        assert len(self.entity_links) > 0, f"Spec does not have any entity links: {self}"
        return TimeDimensionSpec(
            element_name=self.element_name,
            entity_links=self.entity_links[1:],
            time_granularity=self.time_granularity,
            date_part=self.date_part,
        )

    @property
    def without_entity_links(self) -> TimeDimensionSpec:  # noqa: D
        return TimeDimensionSpec.from_name(self.element_name)

    @staticmethod
    def from_name(name: str) -> TimeDimensionSpec:  # noqa: D
        structured_name = StructuredLinkableSpecName.from_name(name)
        return TimeDimensionSpec(
            entity_links=tuple(EntityReference(idl) for idl in structured_name.entity_link_names),
            element_name=structured_name.element_name,
            time_granularity=structured_name.time_granularity or DEFAULT_TIME_GRANULARITY,
        )

    @property
    def reference(self) -> TimeDimensionReference:  # noqa: D
        return TimeDimensionReference(element_name=self.element_name)

    @property
    def dimension_reference(self) -> DimensionReference:  # noqa: D
        return DimensionReference(element_name=self.element_name)

    @property
    def qualified_name(self) -> str:  # noqa: D
        return StructuredLinkableSpecName(
            entity_link_names=tuple(x.element_name for x in self.entity_links),
            element_name=self.element_name,
            time_granularity=self.time_granularity,
            date_part=self.date_part,
        ).qualified_name

    @property
    def as_linkable_spec_set(self) -> LinkableSpecSet:  # noqa: D
        return LinkableSpecSet(time_dimension_specs=(self,))

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_time_dimension_spec(self)

    def with_aggregation_state(self, aggregation_state: AggregationState) -> TimeDimensionSpec:  # noqa: D
        return TimeDimensionSpec(
            element_name=self.element_name,
            entity_links=self.entity_links,
            time_granularity=self.time_granularity,
            date_part=self.date_part,
            aggregation_state=aggregation_state,
        )


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
    def linkable_specs(self) -> LinkableSpecSet:  # noqa: D
        return LinkableSpecSet(
            dimension_specs=(),
            time_dimension_specs=(TimeDimensionSpec.from_name(self.name),),
            entity_specs=tuple(
                LinklessEntitySpec.from_element_name(entity_name) for entity_name in self.window_groupings
            ),
        )

    def __eq__(self, other: Any) -> bool:  # type: ignore[misc] # noqa: D
        if not isinstance(other, NonAdditiveDimensionSpec):
            return False
        return self.bucket_hash == other.bucket_hash


@dataclass(frozen=True)
class MeasureSpec(InstanceSpec):  # noqa: D
    element_name: str
    non_additive_dimension_spec: Optional[NonAdditiveDimensionSpec] = None

    @staticmethod
    def from_name(name: str) -> MeasureSpec:
        """Construct from a name e.g. listing__ds__month."""
        return MeasureSpec(element_name=name)

    @staticmethod
    def from_reference(reference: MeasureReference) -> MeasureSpec:
        """Initialize from a measure reference instance."""
        return MeasureSpec(element_name=reference.element_name)

    @property
    def qualified_name(self) -> str:  # noqa: D
        return self.element_name

    @property
    def as_reference(self) -> MeasureReference:  # noqa: D
        return MeasureReference(element_name=self.element_name)

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_measure_spec(self)


@dataclass(frozen=True)
class MetricSpec(InstanceSpec):  # noqa: D
    # Time-over-time could go here
    element_name: str
    constraint: Optional[WhereFilterSpec] = None
    alias: Optional[str] = None
    offset_window: Optional[PydanticMetricTimeWindow] = None
    offset_to_grain: Optional[TimeGranularity] = None

    @staticmethod
    def from_element_name(element_name: str) -> MetricSpec:  # noqa: D
        return MetricSpec(element_name=element_name)

    @property
    def qualified_name(self) -> str:  # noqa: D
        return self.element_name

    @property
    def as_reference(self) -> MetricReference:  # noqa: D
        return MetricReference(element_name=self.element_name)

    @staticmethod
    def from_reference(reference: MetricReference) -> MetricSpec:
        """Initialize from a metric reference instance."""
        return MetricSpec(element_name=reference.element_name)

    @property
    def alias_spec(self) -> MetricSpec:
        """Returns a MetricSpec represneting the alias state."""
        return MetricSpec(
            element_name=self.alias or self.element_name,
            constraint=self.constraint,
        )

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_metric_spec(self)


@dataclass(frozen=True)
class MetricInputMeasureSpec(SerializableDataclass):
    """The spec for a measure defined as a metric input.

    This is necessary because the MeasureSpec is used as a key linking the measures used in the query
    to the measures defined in the semantic models. Adding metric-specific information, like constraints,
    causes lookups connecting query -> semantic model to fail in strange ways. This spec, then, provides
    both the key (in the form of a MeasureSpec) along with whatever measure-specific attributes
    a user might specify in a metric definition or query accessing the metric itself.

    Note - when specifying a metric comprised of two input instances of the same measure, at least one
    must have a distinct alias, otherwise SQL exceptions may occur. This should be enforced via validation.
    """

    measure_spec: MeasureSpec
    constraint: Optional[WhereFilterSpec] = None
    alias: Optional[str] = None

    @property
    def post_aggregation_spec(self) -> MeasureSpec:
        """Return a MeasureSpec instance representing the post-aggregation spec state for the underlying measure."""
        if self.alias:
            return MeasureSpec(
                element_name=self.alias,
                non_additive_dimension_spec=self.measure_spec.non_additive_dimension_spec,
            )
        else:
            return self.measure_spec


@dataclass(frozen=True)
class OrderBySpec(SerializableDataclass):  # noqa: D
    instance_spec: InstanceSpec
    descending: bool


@dataclass(frozen=True)
class FilterSpec(SerializableDataclass):  # noqa: D
    expr: str
    elements: Tuple[InstanceSpec, ...]


@dataclass(frozen=True)
class LinkableSpecSet(SerializableDataclass):
    """Groups linkable specs."""

    dimension_specs: Tuple[DimensionSpec, ...] = ()
    time_dimension_specs: Tuple[TimeDimensionSpec, ...] = ()
    entity_specs: Tuple[EntitySpec, ...] = ()

    @property
    def as_tuple(self) -> Tuple[LinkableInstanceSpec, ...]:  # noqa: D
        return tuple(itertools.chain(self.dimension_specs, self.time_dimension_specs, self.entity_specs))

    @staticmethod
    def merge(spec_sets: Sequence[LinkableSpecSet]) -> LinkableSpecSet:
        """Merges and dedupes the linkable specs."""
        dimension_specs: List[DimensionSpec] = []
        time_dimension_specs: List[TimeDimensionSpec] = []
        entity_specs: List[EntitySpec] = []

        for spec_set in spec_sets:
            for dimension_spec in spec_set.dimension_specs:
                if dimension_spec not in dimension_specs:
                    dimension_specs.append(dimension_spec)
            for time_dimension_spec in spec_set.time_dimension_specs:
                if time_dimension_spec not in time_dimension_specs:
                    time_dimension_specs.append(time_dimension_spec)
            for entity_spec in spec_set.entity_specs:
                if entity_spec not in entity_specs:
                    entity_specs.append(entity_spec)

        return LinkableSpecSet(
            dimension_specs=tuple(dimension_specs),
            time_dimension_specs=tuple(time_dimension_specs),
            entity_specs=tuple(entity_specs),
        )

    def is_subset_of(self, other_set: LinkableSpecSet) -> bool:  # noqa: D
        return set(self.as_tuple).issubset(set(other_set.as_tuple))

    @property
    def as_spec_set(self) -> InstanceSpecSet:  # noqa: D
        return InstanceSpecSet(
            dimension_specs=self.dimension_specs,
            time_dimension_specs=self.time_dimension_specs,
            entity_specs=self.entity_specs,
        )

    def difference(self, other: LinkableSpecSet) -> LinkableSpecSet:  # noqa: D
        return LinkableSpecSet(
            dimension_specs=tuple(set(self.dimension_specs) - set(other.dimension_specs)),
            time_dimension_specs=tuple(set(self.time_dimension_specs) - set(other.time_dimension_specs)),
            entity_specs=tuple(set(self.entity_specs) - set(other.entity_specs)),
        )

    def __len__(self) -> int:  # noqa: D
        return len(self.dimension_specs) + len(self.time_dimension_specs) + len(self.entity_specs)


@dataclass(frozen=True)
class MetricFlowQuerySpec(SerializableDataclass):
    """Specs needed for running a query."""

    metric_specs: Tuple[MetricSpec, ...] = ()
    dimension_specs: Tuple[DimensionSpec, ...] = ()
    entity_specs: Tuple[EntitySpec, ...] = ()
    time_dimension_specs: Tuple[TimeDimensionSpec, ...] = ()
    order_by_specs: Tuple[OrderBySpec, ...] = ()
    time_range_constraint: Optional[TimeRangeConstraint] = None
    where_constraint: Optional[WhereFilterSpec] = None
    limit: Optional[int] = None

    @property
    def linkable_specs(self) -> LinkableSpecSet:  # noqa: D
        return LinkableSpecSet(
            dimension_specs=self.dimension_specs,
            time_dimension_specs=self.time_dimension_specs,
            entity_specs=self.entity_specs,
        )


TransformOutputT = TypeVar("TransformOutputT")


class InstanceSpecSetTransform(Generic[TransformOutputT], ABC):
    """Function to use for transforming spec sets."""

    @abstractmethod
    def transform(self, spec_set: InstanceSpecSet) -> TransformOutputT:  # noqa: D
        pass


@dataclass(frozen=True)
class InstanceSpecSet(SerializableDataclass):
    """Consolidates all specs used in an instance set."""

    metric_specs: Tuple[MetricSpec, ...] = ()
    measure_specs: Tuple[MeasureSpec, ...] = ()
    dimension_specs: Tuple[DimensionSpec, ...] = ()
    entity_specs: Tuple[EntitySpec, ...] = ()
    time_dimension_specs: Tuple[TimeDimensionSpec, ...] = ()
    metadata_specs: Tuple[MetadataSpec, ...] = ()

    @staticmethod
    def merge(others: Sequence[InstanceSpecSet]) -> InstanceSpecSet:
        """Merge all sets into one set, without de-duplication."""
        return InstanceSpecSet(
            metric_specs=tuple(itertools.chain.from_iterable([x.metric_specs for x in others])),
            measure_specs=tuple(itertools.chain.from_iterable([x.measure_specs for x in others])),
            dimension_specs=tuple(itertools.chain.from_iterable([x.dimension_specs for x in others])),
            entity_specs=tuple(itertools.chain.from_iterable([x.entity_specs for x in others])),
            time_dimension_specs=tuple(itertools.chain.from_iterable([x.time_dimension_specs for x in others])),
            metadata_specs=tuple(itertools.chain.from_iterable([x.metadata_specs for x in others])),
        )

    def dedupe(self) -> InstanceSpecSet:
        """De-duplicates repeated elements.

        TBD: Have merge de-duplicate instead.
        """
        metric_specs_deduped = []
        for metric_spec in self.metric_specs:
            if metric_spec not in metric_specs_deduped:
                metric_specs_deduped.append(metric_spec)

        measure_specs_deduped = []
        for measure_spec in self.measure_specs:
            if measure_spec not in measure_specs_deduped:
                measure_specs_deduped.append(measure_spec)

        dimension_specs_deduped = []
        for dimension_spec in self.dimension_specs:
            if dimension_spec not in dimension_specs_deduped:
                dimension_specs_deduped.append(dimension_spec)

        time_dimension_specs_deduped = []
        for time_dimension_spec in self.time_dimension_specs:
            if time_dimension_spec not in time_dimension_specs_deduped:
                time_dimension_specs_deduped.append(time_dimension_spec)

        entity_specs_deduped = []
        for entity_spec in self.entity_specs:
            if entity_spec not in entity_specs_deduped:
                entity_specs_deduped.append(entity_spec)

        return InstanceSpecSet(
            metric_specs=tuple(metric_specs_deduped),
            measure_specs=tuple(measure_specs_deduped),
            dimension_specs=tuple(dimension_specs_deduped),
            time_dimension_specs=tuple(time_dimension_specs_deduped),
            entity_specs=tuple(entity_specs_deduped),
        )

    @property
    def linkable_specs(self) -> Sequence[LinkableInstanceSpec]:
        """All linkable specs in this set."""
        return list(itertools.chain(self.dimension_specs, self.time_dimension_specs, self.entity_specs))

    @property
    def all_specs(self) -> Sequence[InstanceSpec]:  # noqa: D
        return tuple(
            itertools.chain(
                self.measure_specs,
                self.dimension_specs,
                self.time_dimension_specs,
                self.entity_specs,
                self.metric_specs,
                self.metadata_specs,
            )
        )

    def transform(self, transform_function: InstanceSpecSetTransform[TransformOutputT]) -> TransformOutputT:  # noqa: D
        return transform_function.transform(self)

    @staticmethod
    def create_from_linkable_specs(linkable_specs: Sequence[LinkableInstanceSpec]) -> InstanceSpecSet:  # noqa: D
        return InstanceSpecSet.merge(tuple(x.as_linkable_spec_set.as_spec_set for x in linkable_specs))


@dataclass(frozen=True)
class PartitionSpecSet(SerializableDataclass):
    """Grouping of the linkable specs."""

    dimension_specs: Tuple[DimensionSpec, ...] = ()
    time_dimension_specs: Tuple[TimeDimensionSpec, ...] = ()


logger = logging.getLogger(__name__)


class WhereFilterResolutionException(Exception):  # noqa: D
    pass


@dataclass(frozen=True)
class WhereFilterSpec(SerializableDataclass):
    """Similar to the WhereFilter, but with the where_sql_template rendered and used elements extracted.

    For example:

    WhereFilter(where_sql_template="{{ Dimension('listing__country') }} == 'US'"))

    ->

    ResolvedWhereFilter(
        where_sql="listing__country == 'US'",
        bind_parameters: SqlBindParameters(),
        linkable_spec_set: LinkableSpecSet(
            dimension_specs=(
                DimensionSpec(
                    element_name='country',
                    entity_links=('listing',),
            ),
        )
    )
    """

    # Debating whether where_sql / bind_parameters belongs here. where_sql may become dialect specific if we introduce
    # quoted identifiers later.
    where_sql: str
    bind_parameters: SqlBindParameters
    linkable_spec_set: LinkableSpecSet

    def combine(self, other: WhereFilterSpec) -> WhereFilterSpec:  # noqa: D
        return WhereFilterSpec(
            where_sql=f"({self.where_sql}) AND ({other.where_sql})",
            bind_parameters=self.bind_parameters.combine(other.bind_parameters),
            linkable_spec_set=LinkableSpecSet.merge([self.linkable_spec_set, other.linkable_spec_set]),
        )
