"""Classes required for defining metric definition object instances (see MdoInstance)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Optional, TypeVar, Generic, Sequence, Tuple

from pydantic.generics import GenericModel

from metricflow.column_assoc import ColumnAssociation
from metricflow.specs import (
    MeasureSpec,
    DimensionSpec,
    IdentifierSpec,
    MetricSpec,
    InstanceSpec,
    TimeDimensionSpec,
    TimeDimensionReference,
    InstanceSpecSet,
)
from metricflow.model.objects.utils import FrozenBaseModel


class ModelReference(FrozenBaseModel):
    """A reference to something in the model.

    For example, a measure instance could have a defined_from field that has a model reference to the measure / data
    source that it is supposed to reference. Added for exploratory purposes, so whether this is needed is TBD.
    """

    pass


class DataSourceReference(ModelReference):
    """A reference to a data source definition in the model."""

    data_source_name: str

    def __hash__(self) -> int:  # noqa: D
        return hash(self.data_source_name)


class DataSourceElementReference(ModelReference):
    """A reference to an element definition in a data source definition in the model."""

    data_source_name: str
    element_name: str

    def is_from(self, ref: DataSourceReference) -> bool:
        """Returns true if this reference is from the same data source as the supplied reference."""
        return self.data_source_name == ref.data_source_name


class MetricModelReference(ModelReference):
    """A reference to a metric definition in the model."""

    metric_name: str


# Type for the specification used in the instance.
SpecT = TypeVar("SpecT", bound=InstanceSpec)


# Type for the column association used in the instance.
ColumnNameAssociationT = TypeVar("ColumnNameAssociationT", bound=ColumnAssociation)


class MdoInstance(GenericModel, Generic[SpecT]):
    """An instance of a metric definition object.

    An instance is different from the metric definition object in that it correlates to columns in a data set and can be
    in different states. e.g. a measure instance can be aggregated, or a time dimension can be at a different
    granularity.
    """

    # The columns associated with this instance. Some instances may have multiple columns associated with it, e.g.
    # composite identifiers.
    associated_columns: Tuple[ColumnAssociation, ...]
    # The spec that describes this instance.
    spec: SpecT

    @property
    def associated_column(self) -> ColumnAssociation:
        """Helper for getting the associated column until support for multiple associated columns is added."""
        assert len(self.associated_columns) == 1
        return self.associated_columns[0]


# Instances for the major metric object types


class DataSourceElementInstance(FrozenBaseModel, ABC):  # noqa: D
    # This instance is derived from something defined in a data source.
    defined_from: Tuple[DataSourceElementReference, ...]


class AggregationState(Enum):
    """Represents how the measure is aggregated."""

    # When reading from the source, the measure is considered non-aggregated.
    NON_AGGREGATED = "NON_AGGREGATED"
    PARTIAL = "PARTIAL"
    COMPLETE = "COMPLETE"

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}.{self.name}"


class MeasureInstance(MdoInstance[MeasureSpec], DataSourceElementInstance):  # noqa: D
    aggregation_state: AggregationState
    source_time_dimension_reference: Optional[TimeDimensionReference] = None


class DimensionInstance(MdoInstance[DimensionSpec], DataSourceElementInstance):  # noqa: D
    pass


class TimeDimensionInstance(MdoInstance[TimeDimensionSpec], DataSourceElementInstance):  # noqa: D
    pass


class IdentifierInstance(MdoInstance[IdentifierSpec], DataSourceElementInstance):  # noqa: D
    pass


class MetricInstance(MdoInstance[MetricSpec], FrozenBaseModel):  # noqa: D
    defined_from: Sequence[MetricModelReference]
    pass


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


class InstanceSet(FrozenBaseModel):
    """A set that includes all instance types.

    Generally used to help represent that data that is flowing between nodes in the metric dataflow plan.
    """

    measure_instances: Tuple[MeasureInstance, ...] = ()
    dimension_instances: Tuple[DimensionInstance, ...] = ()
    time_dimension_instances: Tuple[TimeDimensionInstance, ...] = ()
    identifier_instances: Tuple[IdentifierInstance, ...] = ()
    metric_instances: Tuple[MetricInstance, ...] = ()

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
        identifier_instances: List[IdentifierInstance] = []
        metric_instances: List[MetricInstance] = []

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
            for identifier_instance in instance_set.identifier_instances:
                if identifier_instance.spec not in {x.spec for x in identifier_instances}:
                    identifier_instances.append(identifier_instance)
            for metric_instance in instance_set.metric_instances:
                if metric_instance.spec not in {x.spec for x in metric_instances}:
                    metric_instances.append(metric_instance)

        return InstanceSet(
            measure_instances=tuple(measure_instances),
            dimension_instances=tuple(dimension_instances),
            time_dimension_instances=tuple(time_dimension_instances),
            identifier_instances=tuple(identifier_instances),
            metric_instances=tuple(metric_instances),
        )

    @property
    def spec_set(self) -> InstanceSpecSet:  # noqa: D
        return InstanceSpecSet(
            measure_specs=tuple(x.spec for x in self.measure_instances),
            dimension_specs=tuple(x.spec for x in self.dimension_instances),
            time_dimension_specs=tuple(x.spec for x in self.time_dimension_instances),
            identifier_specs=tuple(x.spec for x in self.identifier_instances),
            metric_specs=tuple(x.spec for x in self.metric_instances),
        )
