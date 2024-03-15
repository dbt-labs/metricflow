from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    MetricCallParameterSet,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.references import LinkableElementReference
from typing_extensions import override

from metricflow.specs.patterns.entity_link_pattern import (
    EntityLinkPattern,
    EntityLinkPatternParameterSet,
    ParameterSetField,
)
from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.specs import InstanceSpec, InstanceSpecSet, LinkableInstanceSpec


@dataclass(frozen=True)
class DimensionPattern(EntityLinkPattern):
    """Similar to EntityPathPattern but only matches dimensions / time dimensions.

    Analogous pattern for Dimension() in the object builder naming scheme.
    """

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        spec_set = InstanceSpecSet.from_specs(candidate_specs)
        filtered_specs: Sequence[LinkableInstanceSpec] = spec_set.dimension_specs + spec_set.time_dimension_specs
        return super().match(filtered_specs)

    @staticmethod
    def from_call_parameter_set(  # noqa: D
        dimension_call_parameter_set: DimensionCallParameterSet,
    ) -> DimensionPattern:
        return DimensionPattern(
            parameter_set=EntityLinkPatternParameterSet.from_parameters(
                fields_to_compare=(
                    ParameterSetField.ELEMENT_NAME,
                    ParameterSetField.ENTITY_LINKS,
                ),
                element_name=dimension_call_parameter_set.dimension_reference.element_name,
                group_by_links=dimension_call_parameter_set.entity_path,
            )
        )


@dataclass(frozen=True)
class TimeDimensionPattern(EntityLinkPattern):
    """Similar to EntityPathPattern but only matches time dimensions.

    Analogous pattern for TimeDimension() in the object builder naming scheme.
    """

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        spec_set = InstanceSpecSet.from_specs(candidate_specs)
        return super().match(spec_set.time_dimension_specs)

    @staticmethod
    def from_call_parameter_set(
        time_dimension_call_parameter_set: TimeDimensionCallParameterSet,
    ) -> TimeDimensionPattern:
        """Create the pattern that represents 'TimeDimension(...)' in the object builder naming scheme.

        For this pattern, A None value for the time grain matches any grain. However, a None value for the date part
        means that the date part has to be None. This follows the interface defined by the object builder naming scheme.
        """
        fields_to_compare: List[ParameterSetField] = [
            ParameterSetField.ELEMENT_NAME,
            ParameterSetField.ENTITY_LINKS,
            ParameterSetField.DATE_PART,
        ]

        if time_dimension_call_parameter_set.time_granularity is not None:
            fields_to_compare.append(ParameterSetField.TIME_GRANULARITY)

        return TimeDimensionPattern(
            parameter_set=EntityLinkPatternParameterSet.from_parameters(
                fields_to_compare=tuple(fields_to_compare),
                element_name=time_dimension_call_parameter_set.time_dimension_reference.element_name,
                group_by_links=time_dimension_call_parameter_set.entity_path,
                time_granularity=time_dimension_call_parameter_set.time_granularity,
                date_part=time_dimension_call_parameter_set.date_part,
            )
        )


@dataclass(frozen=True)
class EntityPattern(EntityLinkPattern):
    """Similar to EntityPathPattern but only matches entities.

    Analogous pattern for Entity() in the object builder naming scheme.
    """

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        spec_set = InstanceSpecSet.from_specs(candidate_specs)
        return super().match(spec_set.entity_specs)

    @staticmethod
    def from_call_parameter_set(entity_call_parameter_set: EntityCallParameterSet) -> EntityPattern:  # noqa: D
        return EntityPattern(
            parameter_set=EntityLinkPatternParameterSet.from_parameters(
                fields_to_compare=(
                    ParameterSetField.ELEMENT_NAME,
                    ParameterSetField.ENTITY_LINKS,
                ),
                element_name=entity_call_parameter_set.entity_reference.element_name,
                group_by_links=entity_call_parameter_set.entity_path,
            )
        )


@dataclass(frozen=True)
class GroupByMetricPatternParameterSet:
    """Pattern for joining metrics to semantic models so that they can be used in group bys & filters."""

    # The name of the metric as defined in the semantic manifest.
    element_name: str
    # The group bys used for joining metrics to semantic models.
    group_by_links: Sequence[LinkableElementReference]

    @staticmethod
    def from_parameters(  # noqa: D
        element_name: str, group_by_links: Sequence[LinkableElementReference]
    ) -> GroupByMetricPatternParameterSet:
        return GroupByMetricPatternParameterSet(element_name=element_name, group_by_links=group_by_links)


@dataclass(frozen=True)
class GroupByMetricPattern(SpecPattern):
    """A pattern that matches metrics using the group by specifications."""

    parameter_set: GroupByMetricPatternParameterSet

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        filtered_candidate_specs = InstanceSpecSet.from_specs(candidate_specs).group_by_metric_specs

        matching_specs: List[InstanceSpec] = []
        for spec in filtered_candidate_specs:
            if spec.element_name == self.parameter_set.element_name and {
                # TODO: should these be ordered? tuples instead of sets? Does order matter for matching metrics? thinking no.
                group_by.element_name
                for group_by in spec.group_by_links
            } == {group_by.element_name for group_by in self.parameter_set.group_by_links}:
                matching_specs.append(spec)

        return matching_specs

    @staticmethod
    def from_call_parameter_set(metric_call_parameter_set: MetricCallParameterSet) -> GroupByMetricPattern:  # noqa: D
        return GroupByMetricPattern(
            parameter_set=GroupByMetricPatternParameterSet.from_parameters(
                element_name=metric_call_parameter_set.metric_reference.element_name,
                group_by_links=metric_call_parameter_set.group_by,
            )
        )
