from __future__ import annotations

from dataclasses import dataclass

from metricflow.dataclass_serialization import SerializableDataclass


@dataclass(frozen=True)
class ElementReference(SerializableDataclass):
    """Used when we need to refer to a dimension, measure, entity, but other attributes are unknown."""

    element_name: str


@dataclass(frozen=True)
class LinkableElementReference(ElementReference):
    """Used when we need to refer to a dimension or entity, but other attributes are unknown."""

    pass


@dataclass(frozen=True)
class MeasureReference(ElementReference):
    """Used when we need to refer to a measure (separate from LinkableElementReference because measures aren't linkable"""

    pass


@dataclass(frozen=True)
class DimensionReference(LinkableElementReference):  # noqa: D
    pass

    @property
    def time_dimension_reference(self) -> TimeDimensionReference:  # noqa: D
        return TimeDimensionReference(element_name=self.element_name)


@dataclass(frozen=True)
class EntityReference(LinkableElementReference):  # noqa: D
    pass


@dataclass(frozen=True)
class CompositeSubEntityReference(ElementReference):  # noqa: D
    pass


@dataclass(frozen=True)
class TimeDimensionReference(DimensionReference):  # noqa: D
    pass

    def dimension_reference(self) -> DimensionReference:  # noqa: D
        return DimensionReference(element_name=self.element_name)


@dataclass(frozen=True)
class MetricReference(ElementReference):  # noqa: D
    pass


class ModelReference(SerializableDataclass):
    """A reference to something in the model.

    For example, a measure instance could have a defined_from field that has a model reference to the measure / data
    source that it is supposed to reference. Added for exploratory purposes, so whether this is needed is TBD.
    """

    pass


@dataclass(frozen=True)
class DataSourceReference(ModelReference):
    """A reference to a data source definition in the model."""

    data_source_name: str

    def __hash__(self) -> int:  # noqa: D
        return hash(self.data_source_name)


@dataclass(frozen=True)
class DataSourceElementReference(ModelReference):
    """A reference to an element definition in a data source definition in the model.

    TODO: Fields should be *Reference objects.
    """

    data_source_name: str
    element_name: str

    @staticmethod
    def create_from_references(  # noqa: D
        data_source_reference: DataSourceReference, element_reference: ElementReference
    ) -> DataSourceElementReference:
        return DataSourceElementReference(
            data_source_name=data_source_reference.data_source_name,
            element_name=element_reference.element_name,
        )

    @property
    def data_source_reference(self) -> DataSourceReference:  # noqa: D
        return DataSourceReference(self.data_source_name)

    def is_from(self, ref: DataSourceReference) -> bool:
        """Returns true if this reference is from the same data source as the supplied reference."""
        return self.data_source_name == ref.data_source_name


@dataclass(frozen=True)
class MetricModelReference(ModelReference):
    """A reference to a metric definition in the model."""

    metric_name: str
