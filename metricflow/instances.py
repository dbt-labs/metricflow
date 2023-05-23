"""Classes required for defining metric definition object instances (see MdoInstance)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, List, Tuple, TypeVar

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from dbt_semantic_interfaces.references import MetricModelReference, SemanticModelElementReference

from metricflow.aggregation_properties import AggregationState
from metricflow.specs.column_assoc import ColumnAssociation
from metricflow.specs.specs import (
    DimensionSpec,
    EntitySpec,
    InstanceSpec,
    InstanceSpecSet,
    MeasureSpec,
    MetadataSpec,
    MetricSpec,
    TimeDimensionSpec,
)

# Type for the specification used in the instance.
SpecT = TypeVar("SpecT", bound=InstanceSpec)


# Type for the column association used in the instance.
ColumnNameAssociationT = TypeVar("ColumnNameAssociationT", bound=ColumnAssociation)


class MdoInstance(ABC, Generic[SpecT]):
    """An instance of a metric definition object.

    An instance is different from the metric definition object in that it correlates to columns in a data set and can be
    in different states. e.g. a measure instance can be aggregated, or a time dimension can be at a different
    granularity.
    """

    # The columns associated with this instance.
    associated_columns: Tuple[ColumnAssociation, ...]
    # The spec that describes this instance.
    spec: SpecT

    @property
    def associated_column(self) -> ColumnAssociation:
        """Helper for getting the associated column until support for multiple associated columns is added."""
        assert len(self.associated_columns) == 1
        return self.associated_columns[0]


# Instances for the major metric object types


@dataclass(frozen=True)
class SemanticModelElementInstance(SerializableDataclass):  # noqa: D
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
class MeasureInstance(MdoInstance[MeasureSpec], SemanticModelElementInstance):  # noqa: D
    associated_columns: Tuple[ColumnAssociation, ...]
    spec: MeasureSpec
    aggregation_state: AggregationState


@dataclass(frozen=True)
class DimensionInstance(MdoInstance[DimensionSpec], SemanticModelElementInstance):  # noqa: D
    associated_columns: Tuple[ColumnAssociation, ...]
    spec: DimensionSpec


@dataclass(frozen=True)
class TimeDimensionInstance(MdoInstance[TimeDimensionSpec], SemanticModelElementInstance):  # noqa: D
    associated_columns: Tuple[ColumnAssociation, ...]
    spec: TimeDimensionSpec


@dataclass(frozen=True)
class EntityInstance(MdoInstance[EntitySpec], SemanticModelElementInstance):  # noqa: D
    associated_columns: Tuple[ColumnAssociation, ...]
    spec: EntitySpec


@dataclass(frozen=True)
class MetricInstance(MdoInstance[MetricSpec], SerializableDataclass):  # noqa: D
    associated_columns: Tuple[ColumnAssociation, ...]
    spec: MetricSpec
    defined_from: Tuple[MetricModelReference, ...]


@dataclass(frozen=True)
class MetadataInstance(MdoInstance[MetadataSpec], SerializableDataclass):  # noqa: D
    associated_columns: Tuple[ColumnAssociation, ...]
    spec: MetadataSpec


# Output type of transform function
TransformOutputT = TypeVar("TransformOutputT")


class InstanceSetTransform(Generic[TransformOutputT], ABC):
    """Function to use for transforming instance sets.

    This function interface should be used instead of manually modifying the members of an instance set so that it's
    easy to track, through "Find Usages", all transformations required for instance sets. This should make adding new
    fields (e.g. WeirdDimensionInstance) to instance sets easier to find and reason about.
    """

    @abstractmethod
    def transform(self, instance_set: InstanceSet) -> TransformOutputT:  # noqa: D
        pass


@dataclass(frozen=True)
class InstanceSet(SerializableDataclass):
    """A set that includes all instance types.

    Generally used to help represent that data that is flowing between nodes in the metric dataflow plan.
    """

    measure_instances: Tuple[MeasureInstance, ...] = ()
    dimension_instances: Tuple[DimensionInstance, ...] = ()
    time_dimension_instances: Tuple[TimeDimensionInstance, ...] = ()
    entity_instances: Tuple[EntityInstance, ...] = ()
    metric_instances: Tuple[MetricInstance, ...] = ()
    metadata_instances: Tuple[MetadataInstance, ...] = ()

    def transform(self, transform_function: InstanceSetTransform[TransformOutputT]) -> TransformOutputT:  # noqa: D
        return transform_function.transform(self)

    @staticmethod
    def merge(instance_sets: List[InstanceSet]) -> InstanceSet:
        """Combine all instances from all instances into a single instance set.

        Instances will be de-duped based on their spec.
        """
        measure_instances: List[MeasureInstance] = []
        dimension_instances: List[DimensionInstance] = []
        time_dimension_instances: List[TimeDimensionInstance] = []
        entity_instances: List[EntityInstance] = []
        metric_instances: List[MetricInstance] = []
        metadata_instances: List[MetadataInstance] = []

        for instance_set in instance_sets:
            for measure_instance in instance_set.measure_instances:
                if measure_instance.spec not in {x.spec for x in measure_instances}:
                    measure_instances.append(measure_instance)
            for dimension_instance in instance_set.dimension_instances:
                if dimension_instance.spec not in {x.spec for x in dimension_instances}:
                    dimension_instances.append(dimension_instance)
            for time_dimension_instance in instance_set.time_dimension_instances:
                if time_dimension_instance.spec not in {x.spec for x in time_dimension_instances}:
                    time_dimension_instances.append(time_dimension_instance)
            for entity_instance in instance_set.entity_instances:
                if entity_instance.spec not in {x.spec for x in entity_instances}:
                    entity_instances.append(entity_instance)
            for metric_instance in instance_set.metric_instances:
                if metric_instance.spec not in {x.spec for x in metric_instances}:
                    metric_instances.append(metric_instance)
            for metadata_instance in instance_set.metadata_instances:
                if metadata_instance.spec not in {x.spec for x in metadata_instances}:
                    metadata_instances.append(metadata_instance)

        return InstanceSet(
            measure_instances=tuple(measure_instances),
            dimension_instances=tuple(dimension_instances),
            time_dimension_instances=tuple(time_dimension_instances),
            entity_instances=tuple(entity_instances),
            metric_instances=tuple(metric_instances),
            metadata_instances=tuple(metadata_instances),
        )

    @property
    def spec_set(self) -> InstanceSpecSet:  # noqa: D
        return InstanceSpecSet(
            measure_specs=tuple(x.spec for x in self.measure_instances),
            dimension_specs=tuple(x.spec for x in self.dimension_instances),
            time_dimension_specs=tuple(x.spec for x in self.time_dimension_instances),
            entity_specs=tuple(x.spec for x in self.entity_instances),
            metric_specs=tuple(x.spec for x in self.metric_instances),
            metadata_specs=tuple(x.spec for x in self.metadata_instances),
        )
