from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Sequence, Tuple

from dbt_semantic_interfaces.references import EntityReference, SemanticModelReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from typing_extensions import override

from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.experimental.comparison import Comparable, ComparisonAnyType


@dataclass(frozen=True)
class ComputationMethod(Comparable, ABC):
    """Describes how to compute a semantic graph node from another node"""

    @property
    @abstractmethod
    def comparison_tuple(self) -> Tuple[ComparisonAnyType, ...]:
        raise NotImplementedError

    @property
    @abstractmethod
    def dot_label(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        raise NotImplementedError


@dataclass(frozen=True)
class CoLocatedComputationMethod(ComputationMethod):
    """Describes computing an entity / entity attribute by using the same rows from a common semantic model."""

    semantic_model_reference: SemanticModelReference

    @property
    def comparison_tuple(self) -> Tuple[ComparisonAnyType, ...]:
        return (self.semantic_model_reference,)

    @property
    @override
    def dot_label(self) -> str:
        return f"Co-located in {repr(self.semantic_model_reference.semantic_model_name)}"

    @property
    @override
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        return (
            DisplayedProperty("via", "COLOCATED"),
            DisplayedProperty("model", self.semantic_model_reference.semantic_model_name),
        )


@dataclass(frozen=True)
class JoinedComputationMethod(ComputationMethod):
    """Describes computing an entity / entity attribute by joining two semantic models."""

    left_semantic_model_reference: SemanticModelReference
    right_semantic_model_reference: SemanticModelReference
    on_entity_reference: EntityReference

    @property
    @override
    def comparison_tuple(self) -> Tuple[ComparisonAnyType, ...]:
        return (self.left_semantic_model_reference, self.right_semantic_model_reference, self.on_entity_reference)

    @property
    @override
    def dot_label(self) -> str:
        return (
            f"({repr(self.left_semantic_model_reference.semantic_model_name)} JOIN "
            f"{repr(self.right_semantic_model_reference.semantic_model_name)} ON "
            f"{repr(self.on_entity_reference.element_name)})"
        )

    @property
    @override
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        return (
            DisplayedProperty("via", "JOINED"),
            DisplayedProperty("left_model", self.left_semantic_model_reference.semantic_model_name),
            DisplayedProperty("right_model", self.right_semantic_model_reference.semantic_model_name),
        )


@dataclass(frozen=True)
class MetricTimeComputationMethod(ComputationMethod):
    @property
    @override
    def comparison_tuple(self) -> Tuple[ComparisonAnyType, ...]:
        return ()

    @property
    @override
    def dot_label(self) -> str:
        return "METRIC_TIME"

    @property
    @override
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        return (DisplayedProperty("via", "METRIC_TIME"),)


@dataclass(frozen=True)
class DateTruncComputationMethod(ComputationMethod):
    time_grain: TimeGranularity

    @property
    @override
    def comparison_tuple(self) -> Tuple[ComparisonAnyType, ...]:
        return (self.time_grain,)

    @property
    @override
    def dot_label(self) -> str:
        return f"DATE_TRUNC({self.time_grain.value!r})"

    @property
    @override
    def displayed_properties(self) -> Sequence[DisplayedProperty]:
        return (DisplayedProperty("via", "DATE_TRUNC"), DisplayedProperty("grain", self.time_grain.value))

    # def __lt__(self, other: ComparisonAnyType) -> bool:  # noqa: D105
    #     if not isinstance(other, ComputationMethod):
    #         return NotImplemented
    #     if not isinstance(other, self.__class__):
    #         return self.__class__.__name__ < other.__class__.__name__
    #     self_comparison_key = (self.left_semantic_model_reference, self.join_type, self.right_semantic_model_reference)
    #     other_comparison_key = (
    #         other.left_semantic_model_reference,
    #         other.join_type,
    #         other.right_semantic_model_reference,
    #     )
    #     return self_comparison_key < other_comparison_key


# @dataclass(frozen=True)
# class UnknownComputationMethod(ComputationMethod):
#     """Placeholder for a computation method that needs to be resolved."""
#
#     @property
#     def comparison_tuple(self) -> Tuple[ComparisonAnyType, ...]:
#         return ()
