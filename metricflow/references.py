from __future__ import annotations

from dataclasses import dataclass

from metricflow.dataclass_serialization import SerializableDataclass


@dataclass(frozen=True)
class ElementReference(SerializableDataclass):
    """Used when we need to refer to a dimension, measure, identifier, but other attributes are unknown."""

    element_name: str


@dataclass(frozen=True)
class LinkableElementReference(ElementReference):
    """Used when we need to refer to a dimension or identifier, but other attributes are unknown."""

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
class IdentifierReference(LinkableElementReference):  # noqa: D
    pass


@dataclass(frozen=True)
class CompositeSubIdentifierReference(ElementReference):  # noqa: D
    pass


@dataclass(frozen=True)
class TimeDimensionReference(DimensionReference):  # noqa: D
    pass

    def dimension_reference(self) -> DimensionReference:  # noqa: D
        return DimensionReference(element_name=self.element_name)
