"""Classes required for defining metric definition object instances (see MdoInstance)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Generic, List, Sequence, Tuple, TypeVar

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from dbt_semantic_interfaces.references import EntityReference, MetricModelReference, SemanticModelElementReference
from typing_extensions import override

from metricflow_semantics.aggregation_properties import AggregationState
from metricflow_semantics.specs.column_assoc import ColumnAssociation, ColumnAssociationResolver
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.specs.metadata_spec import MetadataSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.simple_metric_input_spec import SimpleMetricInputSpec
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.toolkit.visitor import VisitorOutputT

# Type for the specification used in the instance.
SpecT = TypeVar("SpecT", bound=InstanceSpec)


# Type for the column association used in the instance.
ColumnNameAssociationT = TypeVar("ColumnNameAssociationT", bound=ColumnAssociation)


class MdoInstance(ABC, Generic[SpecT]):
    """An instance of a metric definition object.

    An instance is different from the metric definition object in that it correlates to columns in a data set and can be
    in different states. e.g. simple-metric-input instance can be aggregated, or a time dimension can be at a different
    granularity.
    """

    # The columns associated with this instance.
    # TODO: if poss, remove this and instead add a method that resolves this from the spec + column association resolver
    # (ensure we're using consistent logic everywhere so this bug doesn't happen again)
    associated_columns: Tuple[ColumnAssociation, ...]
    # The spec that describes this instance.
    spec: SpecT

    @property
    def associated_column(self) -> ColumnAssociation:
        """Helper for getting the associated column until support for multiple associated columns is added."""
        assert (
            len(self.associated_columns) == 1
        ), f"Expected exactly one column for {self.__class__.__name__}, but got {self.associated_columns}"
        return self.associated_columns[0]

    def accept(self, visitor: InstanceVisitor[VisitorOutputT]) -> VisitorOutputT:
        """See Visitable."""
        raise NotImplementedError

    def with_new_spec(self, new_spec: SpecT, column_association_resolver: ColumnAssociationResolver) -> MdoInstance:
        """Returns a new instance with the spec replaced."""
        raise NotImplementedError


class LinkableInstance(MdoInstance, Generic[SpecT]):
    """An MdoInstance whose spec is linkable (i.e., it can have entity links)."""

    def with_entity_prefix(
        self, entity_prefix: EntityReference, column_association_resolver: ColumnAssociationResolver
    ) -> MdoInstance:
        """Add entity link to the underlying spec and associated column."""
        raise NotImplementedError


# Instances for the major metric object types


@dataclass(frozen=True)
class SemanticModelElementInstance(SerializableDataclass):  # noqa: D101
    # This instance is derived from something defined in a semantic model.
    defined_from: Tuple[SemanticModelElementReference, ...]

    @property
    def origin_semantic_model_reference(self) -> SemanticModelElementReference:
        """Property to grab the element reference pointing to the origin semantic model for this element instance.

        By convention this is the zeroth element in the Tuple. At this time these tuples are always of exactly
        length 1, so the simple assertions here work.

        TODO: make this a required input value, rather than a derived property on these objects
        """
        if len(self.defined_from) != 1:
            raise ValueError(
                f"SemanticModelElementInstances should have exactly one entry in the `defined_from` property, because "
                f"otherwise there is no way to ensure that the first element is always the origin semantic model! Found "
                f"{len(self.defined_from)} elements in this particular instance: {self.defined_from}."
            )

        return self.defined_from[0]


@dataclass(frozen=True)
class SimpleMetricInputInstance(MdoInstance[SimpleMetricInputSpec], SemanticModelElementInstance):  # noqa: D101
    associated_columns: Tuple[ColumnAssociation, ...]
    spec: SimpleMetricInputSpec
    aggregation_state: AggregationState

    def accept(self, visitor: InstanceVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_simple_metric_input_instance(self)

    def with_new_spec(
        self, new_spec: SimpleMetricInputSpec, column_association_resolver: ColumnAssociationResolver
    ) -> SimpleMetricInputInstance:
        """Returns a new instance with the spec replaced."""
        return SimpleMetricInputInstance(
            associated_columns=(column_association_resolver.resolve_spec(new_spec),),
            defined_from=self.defined_from,
            spec=new_spec,
            aggregation_state=self.aggregation_state,
        )


@dataclass(frozen=True)
class DimensionInstance(LinkableInstance[DimensionSpec], SemanticModelElementInstance):  # noqa: D101
    associated_columns: Tuple[ColumnAssociation, ...]
    spec: DimensionSpec

    def accept(self, visitor: InstanceVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_dimension_instance(self)

    def with_entity_prefix(
        self, entity_prefix: EntityReference, column_association_resolver: ColumnAssociationResolver
    ) -> DimensionInstance:
        """Returns a new instance with the entity prefix added to the entity links."""
        transformed_spec = self.spec.with_entity_prefix(entity_prefix)
        return DimensionInstance(
            associated_columns=(column_association_resolver.resolve_spec(transformed_spec),),
            defined_from=self.defined_from,
            spec=transformed_spec,
        )

    def with_new_spec(
        self, new_spec: DimensionSpec, column_association_resolver: ColumnAssociationResolver
    ) -> DimensionInstance:
        """Returns a new instance with the spec replaced."""
        return DimensionInstance(
            associated_columns=(column_association_resolver.resolve_spec(new_spec),),
            defined_from=self.defined_from,
            spec=new_spec,
        )


@dataclass(frozen=True)
class TimeDimensionInstance(LinkableInstance[TimeDimensionSpec], SemanticModelElementInstance):  # noqa: D101
    associated_columns: Tuple[ColumnAssociation, ...]
    spec: TimeDimensionSpec

    def accept(self, visitor: InstanceVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_time_dimension_instance(self)

    def with_entity_prefix(
        self, entity_prefix: EntityReference, column_association_resolver: ColumnAssociationResolver
    ) -> TimeDimensionInstance:
        """Returns a new instance with the entity prefix added to the entity links."""
        transformed_spec = self.spec.with_entity_prefix(entity_prefix)
        return self.with_new_spec(transformed_spec, column_association_resolver)

    def with_new_defined_from(self, defined_from: Sequence[SemanticModelElementReference]) -> TimeDimensionInstance:
        """Returns a new instance with the defined_from field replaced."""
        return TimeDimensionInstance(
            associated_columns=self.associated_columns, defined_from=tuple(defined_from), spec=self.spec
        )

    def with_new_spec(
        self, new_spec: TimeDimensionSpec, column_association_resolver: ColumnAssociationResolver
    ) -> TimeDimensionInstance:
        """Returns a new instance with the spec replaced."""
        return TimeDimensionInstance(
            associated_columns=(column_association_resolver.resolve_spec(new_spec),),
            defined_from=self.defined_from,
            spec=new_spec,
        )


@dataclass(frozen=True)
class EntityInstance(LinkableInstance[EntitySpec], SemanticModelElementInstance):  # noqa: D101
    associated_columns: Tuple[ColumnAssociation, ...]
    spec: EntitySpec

    def accept(self, visitor: InstanceVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_entity_instance(self)

    def with_entity_prefix(
        self, entity_prefix: EntityReference, column_association_resolver: ColumnAssociationResolver
    ) -> EntityInstance:
        """Returns a new instance with the entity prefix added to the entity links."""
        transformed_spec = self.spec.with_entity_prefix(entity_prefix)
        return EntityInstance(
            associated_columns=(column_association_resolver.resolve_spec(transformed_spec),),
            defined_from=self.defined_from,
            spec=transformed_spec,
        )

    def with_new_spec(
        self, new_spec: EntitySpec, column_association_resolver: ColumnAssociationResolver
    ) -> EntityInstance:
        """Returns a new instance with the spec replaced."""
        return EntityInstance(
            associated_columns=(column_association_resolver.resolve_spec(new_spec),),
            defined_from=self.defined_from,
            spec=new_spec,
        )


@dataclass(frozen=True)
class GroupByMetricInstance(LinkableInstance[GroupByMetricSpec], SerializableDataclass):  # noqa: D101
    associated_columns: Tuple[ColumnAssociation, ...]
    spec: GroupByMetricSpec
    defined_from: MetricModelReference

    def accept(self, visitor: InstanceVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_group_by_metric_instance(self)

    def with_entity_prefix(
        self, entity_prefix: EntityReference, column_association_resolver: ColumnAssociationResolver
    ) -> GroupByMetricInstance:
        """Returns a new instance with the entity prefix added to the entity links."""
        transformed_spec = self.spec.with_entity_prefix(entity_prefix)
        return GroupByMetricInstance(
            associated_columns=(column_association_resolver.resolve_spec(transformed_spec),),
            defined_from=self.defined_from,
            spec=transformed_spec,
        )

    def with_new_spec(
        self, new_spec: GroupByMetricSpec, column_association_resolver: ColumnAssociationResolver
    ) -> GroupByMetricInstance:
        """Returns a new instance with the spec replaced."""
        return GroupByMetricInstance(
            associated_columns=(column_association_resolver.resolve_spec(new_spec),),
            defined_from=self.defined_from,
            spec=new_spec,
        )


@dataclass(frozen=True)
class MetricInstance(MdoInstance[MetricSpec], SerializableDataclass):  # noqa: D101
    associated_columns: Tuple[ColumnAssociation, ...]
    spec: MetricSpec
    defined_from: MetricModelReference

    def accept(self, visitor: InstanceVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_metric_instance(self)

    def with_new_spec(
        self, new_spec: MetricSpec, column_association_resolver: ColumnAssociationResolver
    ) -> MetricInstance:
        """Returns a new instance with the spec replaced."""
        return MetricInstance(
            associated_columns=(column_association_resolver.resolve_spec(new_spec),),
            defined_from=self.defined_from,
            spec=new_spec,
        )


@dataclass(frozen=True)
class MetadataInstance(MdoInstance[MetadataSpec], SerializableDataclass):  # noqa: D101
    associated_columns: Tuple[ColumnAssociation, ...]
    spec: MetadataSpec

    def accept(self, visitor: InstanceVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_metadata_instance(self)

    def with_new_spec(
        self, new_spec: MetadataSpec, column_association_resolver: ColumnAssociationResolver
    ) -> MetadataInstance:
        """Returns a new instance with the spec replaced."""
        return MetadataInstance(
            associated_columns=(column_association_resolver.resolve_spec(new_spec),),
            spec=new_spec,
        )


# Output type of transform function
TransformOutputT = TypeVar("TransformOutputT")


class InstanceSetTransform(Generic[TransformOutputT], ABC):
    """Function to use for transforming instance sets.

    This function interface should be used instead of manually modifying the members of an instance set so that it's
    easy to track, through "Find Usages", all transformations required for instance sets. This should make adding new
    fields (e.g. WeirdDimensionInstance) to instance sets easier to find and reason about.
    """

    @abstractmethod
    def transform(self, instance_set: InstanceSet) -> TransformOutputT:  # noqa: D102
        pass


@dataclass(frozen=True)
class InstanceSet(SerializableDataclass):
    """A set that includes all instance types.

    Generally used to help represent that data that is flowing between nodes in the metric dataflow plan.
    """

    simple_metric_input_instances: Tuple[SimpleMetricInputInstance, ...] = ()
    dimension_instances: Tuple[DimensionInstance, ...] = ()
    time_dimension_instances: Tuple[TimeDimensionInstance, ...] = ()
    entity_instances: Tuple[EntityInstance, ...] = ()
    group_by_metric_instances: Tuple[GroupByMetricInstance, ...] = ()
    metric_instances: Tuple[MetricInstance, ...] = ()
    metadata_instances: Tuple[MetadataInstance, ...] = ()

    def transform(self, transform_function: InstanceSetTransform[TransformOutputT]) -> TransformOutputT:  # noqa: D102
        return transform_function.transform(self)

    @staticmethod
    def merge(instance_sets: List[InstanceSet]) -> InstanceSet:
        """Combine all instances from all instances into a single instance set.

        Instances will be de-duped based on their spec.
        """
        simple_metric_input_instances: List[SimpleMetricInputInstance] = []
        dimension_instances: List[DimensionInstance] = []
        time_dimension_instances: List[TimeDimensionInstance] = []
        entity_instances: List[EntityInstance] = []
        group_by_metric_instances: List[GroupByMetricInstance] = []
        metric_instances: List[MetricInstance] = []
        metadata_instances: List[MetadataInstance] = []

        for instance_set in instance_sets:
            for simple_metric_input_instance in instance_set.simple_metric_input_instances:
                if simple_metric_input_instance.spec not in {x.spec for x in simple_metric_input_instances}:
                    simple_metric_input_instances.append(simple_metric_input_instance)
            for dimension_instance in instance_set.dimension_instances:
                if dimension_instance.spec not in {x.spec for x in dimension_instances}:
                    dimension_instances.append(dimension_instance)
            for time_dimension_instance in instance_set.time_dimension_instances:
                if time_dimension_instance.spec not in {x.spec for x in time_dimension_instances}:
                    time_dimension_instances.append(time_dimension_instance)
            for entity_instance in instance_set.entity_instances:
                if entity_instance.spec not in {x.spec for x in entity_instances}:
                    entity_instances.append(entity_instance)
            for group_by_metric_instance in instance_set.group_by_metric_instances:
                if group_by_metric_instance.spec not in {x.spec for x in group_by_metric_instances}:
                    group_by_metric_instances.append(group_by_metric_instance)
            for metric_instance in instance_set.metric_instances:
                if metric_instance.spec not in {x.spec for x in metric_instances}:
                    metric_instances.append(metric_instance)
            for metadata_instance in instance_set.metadata_instances:
                if metadata_instance.spec not in {x.spec for x in metadata_instances}:
                    metadata_instances.append(metadata_instance)

        return InstanceSet(
            simple_metric_input_instances=tuple(simple_metric_input_instances),
            dimension_instances=tuple(dimension_instances),
            time_dimension_instances=tuple(time_dimension_instances),
            entity_instances=tuple(entity_instances),
            group_by_metric_instances=tuple(group_by_metric_instances),
            metric_instances=tuple(metric_instances),
            metadata_instances=tuple(metadata_instances),
        )

    @property
    def spec_set(self) -> InstanceSpecSet:  # noqa: D102
        return InstanceSpecSet(
            simple_metric_input_specs=tuple(x.spec for x in self.simple_metric_input_instances),
            dimension_specs=tuple(x.spec for x in self.dimension_instances),
            time_dimension_specs=tuple(x.spec for x in self.time_dimension_instances),
            entity_specs=tuple(x.spec for x in self.entity_instances),
            group_by_metric_specs=tuple(x.spec for x in self.group_by_metric_instances),
            metric_specs=tuple(x.spec for x in self.metric_instances),
            metadata_specs=tuple(x.spec for x in self.metadata_instances),
        )

    @property
    def as_tuple(self) -> Tuple[MdoInstance, ...]:  # noqa: D102
        return (
            self.simple_metric_input_instances
            + self.dimension_instances
            + self.time_dimension_instances
            + self.entity_instances
            + self.group_by_metric_instances
            + self.metric_instances
            + self.metadata_instances
        )

    @property
    def linkable_instances(self) -> Tuple[LinkableInstance, ...]:  # noqa: D102
        return (
            self.dimension_instances
            + self.time_dimension_instances
            + self.entity_instances
            + self.group_by_metric_instances
        )

    def without_simple_metric_inputs(self) -> InstanceSet:
        """Return a copy of this without the simple-metric input instances."""
        return InstanceSet(
            simple_metric_input_instances=(),
            dimension_instances=self.dimension_instances,
            time_dimension_instances=self.time_dimension_instances,
            entity_instances=self.entity_instances,
            group_by_metric_instances=self.group_by_metric_instances,
            metric_instances=self.metric_instances,
            metadata_instances=self.metadata_instances,
        )


class InstanceVisitor(Generic[VisitorOutputT], ABC):
    """Visitor for the Instance classes."""

    @abstractmethod
    def visit_simple_metric_input_instance(self, instance: SimpleMetricInputInstance) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_dimension_instance(self, dimension_instance: DimensionInstance) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_time_dimension_instance(  # noqa: D102
        self, time_dimension_instance: TimeDimensionInstance
    ) -> VisitorOutputT:
        raise NotImplementedError

    @abstractmethod
    def visit_entity_instance(self, entity_instance: EntityInstance) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_group_by_metric_instance(  # noqa: D102
        self, group_by_metric_instance: GroupByMetricInstance
    ) -> VisitorOutputT:
        raise NotImplementedError

    @abstractmethod
    def visit_metric_instance(self, metric_instance: MetricInstance) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_metadata_instance(self, metadata_instance: MetadataInstance) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError


@dataclass
class _GroupInstanceByTypeVisitor(InstanceVisitor[None]):
    """Group instances by type into an `InstanceSet`."""

    metric_instances: List[MetricInstance] = field(default_factory=list)
    simple_metric_input_instances: List[SimpleMetricInputInstance] = field(default_factory=list)
    dimension_instances: List[DimensionInstance] = field(default_factory=list)
    entity_instances: List[EntityInstance] = field(default_factory=list)
    time_dimension_instances: List[TimeDimensionInstance] = field(default_factory=list)
    group_by_metric_instances: List[GroupByMetricInstance] = field(default_factory=list)
    metadata_instances: List[MetadataInstance] = field(default_factory=list)

    @override
    def visit_simple_metric_input_instance(self, instance: SimpleMetricInputInstance) -> None:
        self.simple_metric_input_instances.append(instance)

    @override
    def visit_dimension_instance(self, dimension_instance: DimensionInstance) -> None:
        self.dimension_instances.append(dimension_instance)

    @override
    def visit_time_dimension_instance(self, time_dimension_instance: TimeDimensionInstance) -> None:
        self.time_dimension_instances.append(time_dimension_instance)

    @override
    def visit_entity_instance(self, entity_instance: EntityInstance) -> None:
        self.entity_instances.append(entity_instance)

    @override
    def visit_group_by_metric_instance(self, group_by_metric_instance: GroupByMetricInstance) -> None:
        self.group_by_metric_instances.append(group_by_metric_instance)

    @override
    def visit_metric_instance(self, metric_instance: MetricInstance) -> None:
        self.metric_instances.append(metric_instance)

    @override
    def visit_metadata_instance(self, metadata_instance: MetadataInstance) -> None:
        self.metadata_instances.append(metadata_instance)


def group_instances_by_type(instances: Sequence[MdoInstance]) -> InstanceSet:
    """Groups a sequence of instances by type."""
    grouper = _GroupInstanceByTypeVisitor()
    for instance in instances:
        instance.accept(grouper)

    return InstanceSet(
        metric_instances=tuple(grouper.metric_instances),
        simple_metric_input_instances=tuple(grouper.simple_metric_input_instances),
        dimension_instances=tuple(grouper.dimension_instances),
        entity_instances=tuple(grouper.entity_instances),
        time_dimension_instances=tuple(grouper.time_dimension_instances),
        group_by_metric_instances=tuple(grouper.group_by_metric_instances),
        metadata_instances=tuple(grouper.metadata_instances),
    )
