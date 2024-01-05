from __future__ import annotations

from abc import ABC, abstractmethod

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    TimeDimensionCallParameterSet,
)
from typing_extensions import override

from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.patterns.typed_patterns import DimensionPattern, EntityPattern, TimeDimensionPattern


class WhereFilterPatternFactory(ABC):
    """Interface that defines how spec patterns should be generated for the group-by-items specified in filters."""

    @abstractmethod
    def create_for_dimension_call_parameter_set(  # noqa: D
        self, dimension_call_parameter_set: DimensionCallParameterSet
    ) -> SpecPattern:
        raise NotImplementedError

    @abstractmethod
    def create_for_time_dimension_call_parameter_set(  # noqa: D
        self, time_dimension_call_parameter_set: TimeDimensionCallParameterSet
    ) -> SpecPattern:
        raise NotImplementedError

    @abstractmethod
    def create_for_entity_call_parameter_set(  # noqa: D
        self, entity_call_parameter_set: EntityCallParameterSet
    ) -> SpecPattern:
        raise NotImplementedError


class DefaultWhereFilterPatternFactory(WhereFilterPatternFactory):
    """Default implementation using patterns derived from EntityLinkPattern."""

    @override
    def create_for_dimension_call_parameter_set(
        self, dimension_call_parameter_set: DimensionCallParameterSet
    ) -> SpecPattern:
        return DimensionPattern.from_call_parameter_set(dimension_call_parameter_set)

    @override
    def create_for_time_dimension_call_parameter_set(
        self, time_dimension_call_parameter_set: TimeDimensionCallParameterSet
    ) -> SpecPattern:
        return TimeDimensionPattern.from_call_parameter_set(time_dimension_call_parameter_set)

    @override
    def create_for_entity_call_parameter_set(self, entity_call_parameter_set: EntityCallParameterSet) -> SpecPattern:
        return EntityPattern.from_call_parameter_set(entity_call_parameter_set)
