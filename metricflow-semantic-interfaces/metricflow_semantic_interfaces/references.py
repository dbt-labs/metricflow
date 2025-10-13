from __future__ import annotations

from dataclasses import dataclass

from metricflow_semantic_interfaces.dataclass_serialization import SerializableDataclass


@dataclass(frozen=True, order=True)
class ElementReference(SerializableDataclass):
    """Used when we need to refer to a dimension, measure, entity, but other attributes are unknown."""

    element_name: str


@dataclass(frozen=True, order=True)
class LinkableElementReference(ElementReference):
    """Used when we need to refer to a dimension or entity, but other attributes are unknown."""

    pass


@dataclass(frozen=True, order=True)
class MeasureReference(ElementReference):
    """Used when we need to refer to a measure.

    This is separate from LinkableElementReference because measures aren't linkable.
    """

    pass


@dataclass(frozen=True, order=True)
class DimensionReference(LinkableElementReference):  # noqa: D101
    pass

    @property
    def time_dimension_reference(self) -> TimeDimensionReference:  # noqa: D102
        return TimeDimensionReference(element_name=self.element_name)


@dataclass(frozen=True, order=True)
class EntityReference(LinkableElementReference):  # noqa: D101
    pass


@dataclass(frozen=True, order=True)
class TimeDimensionReference(DimensionReference):  # noqa: D101
    pass

    @property
    def dimension_reference(self) -> DimensionReference:  # noqa: D102
        return DimensionReference(element_name=self.element_name)


@dataclass(frozen=True, order=True)
class MetricReference(ElementReference):  # noqa: D101
    pass


@dataclass(frozen=True)
class GroupByMetricReference(LinkableElementReference):
    """Represents a group by metric.

    Different from MetricReference because it inherits linkable element attributes.
    """

    pass


@dataclass(frozen=True, order=True)
class ModelReference(SerializableDataclass):
    """A reference to something in the model.

    For example, a measure instance could have a defined_from field that has a model reference to the measure / data
    source that it is supposed to reference. Added for exploratory purposes, so whether this is needed is TBD.
    """

    pass


@dataclass(frozen=True, order=True)
class SemanticModelReference(ModelReference):
    """A reference to a semantic model definition in the model."""

    semantic_model_name: str


@dataclass(frozen=True, order=True)
class SemanticModelElementReference(ModelReference):
    """A reference to an element definition in a semantic model definition in the model.

    TODO: Fields should be *Reference objects.
    """

    semantic_model_name: str
    element_name: str

    @staticmethod
    def create_from_references(  # noqa: D102
        semantic_model_reference: SemanticModelReference, element_reference: ElementReference
    ) -> SemanticModelElementReference:
        return SemanticModelElementReference(
            semantic_model_name=semantic_model_reference.semantic_model_name,
            element_name=element_reference.element_name,
        )

    @property
    def semantic_model_reference(self) -> SemanticModelReference:  # noqa: D102
        return SemanticModelReference(self.semantic_model_name)

    def is_from(self, ref: SemanticModelReference) -> bool:
        """Returns true if this reference is from the same semantic model as the supplied reference."""
        return self.semantic_model_name == ref.semantic_model_name


@dataclass(frozen=True, order=True)
class MetricModelReference(ModelReference):
    """A reference to a metric definition in the model."""

    metric_name: str
