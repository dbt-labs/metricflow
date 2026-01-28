from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    MetricCallParameterSet,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from typing_extensions import override

from metricflow_semantics.errors.error_classes import UnableToSatisfyQueryError
from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
from metricflow_semantics.specs.instance_spec import InstanceSpec, LinkableInstanceSpec
from metricflow_semantics.specs.patterns.entity_link_pattern import (
    EntityLinkPattern,
    ParameterSetField,
    SpecPatternParameterSet,
)
from metricflow_semantics.specs.spec_set import group_specs_by_type


@dataclass(frozen=True)
class DimensionPattern(EntityLinkPattern):
    """Similar to EntityPathPattern but only matches dimensions / time dimensions.

    Analogous pattern for Dimension() in the object builder naming scheme.
    """

    include_time_dimensions: bool = True

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        spec_set = group_specs_by_type(candidate_specs)
        filtered_specs: Tuple[LinkableInstanceSpec, ...] = spec_set.dimension_specs
        if self.include_time_dimensions:
            filtered_specs += spec_set.time_dimension_specs
        return super().match(filtered_specs)

    @staticmethod
    def from_call_parameter_set(  # noqa: D102
        dimension_call_parameter_set: DimensionCallParameterSet,
        include_time_dimensions: bool = True,
    ) -> DimensionPattern:
        return DimensionPattern(
            parameter_set=SpecPatternParameterSet.from_parameters(
                fields_to_compare=(
                    ParameterSetField.ELEMENT_NAME,
                    ParameterSetField.ENTITY_LINKS,
                    ParameterSetField.DATE_PART,
                ),
                element_name=dimension_call_parameter_set.dimension_reference.element_name,
                entity_links=dimension_call_parameter_set.entity_path,
                descending=dimension_call_parameter_set.descending,
            ),
            include_time_dimensions=include_time_dimensions,
        )

    @property
    @override
    def element_pre_filter(self) -> GroupByItemSetFilter:
        return super().element_pre_filter.merge(
            GroupByItemSetFilter.create(any_properties_denylist=(GroupByItemProperty.METRIC,))
        )


@dataclass(frozen=True)
class TimeDimensionPattern(EntityLinkPattern):
    """Similar to EntityPathPattern but only matches time dimensions.

    Analogous pattern for TimeDimension() in the object builder naming scheme.
    """

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        spec_set = group_specs_by_type(candidate_specs)
        return super().match(spec_set.time_dimension_specs)

    @staticmethod
    def get_fields_to_compare(
        time_granularity_name: Optional[str], date_part: Optional[DatePart]
    ) -> Sequence[ParameterSetField]:
        """Select which fields to compare based on inputs."""
        fields_to_compare: Tuple[ParameterSetField, ...] = (
            ParameterSetField.ELEMENT_NAME,
            ParameterSetField.ENTITY_LINKS,
            ParameterSetField.DATE_PART,
        )

        # If date part is requested, time granularity should be ignored.
        if date_part is None and time_granularity_name is not None:
            fields_to_compare += (ParameterSetField.TIME_GRANULARITY,)

        return fields_to_compare

    @classmethod
    def from_call_parameter_set(
        cls,
        time_dimension_call_parameter_set: TimeDimensionCallParameterSet,
    ) -> TimeDimensionPattern:
        """Create the pattern that represents 'TimeDimension(...)' in the object builder naming scheme.

        For this pattern, A None value for the time grain matches any grain. However, a None value for the date part
        means that the date part has to be None. This follows the interface defined by the object builder naming scheme.
        """
        return TimeDimensionPattern(
            parameter_set=SpecPatternParameterSet.from_parameters(
                fields_to_compare=cls.get_fields_to_compare(
                    time_granularity_name=time_dimension_call_parameter_set.time_granularity_name,
                    date_part=time_dimension_call_parameter_set.date_part,
                ),
                element_name=time_dimension_call_parameter_set.time_dimension_reference.element_name,
                entity_links=time_dimension_call_parameter_set.entity_path,
                time_granularity_name=time_dimension_call_parameter_set.time_granularity_name,
                date_part=time_dimension_call_parameter_set.date_part,
                descending=time_dimension_call_parameter_set.descending,
            )
        )

    @property
    @override
    def element_pre_filter(self) -> GroupByItemSetFilter:
        return super().element_pre_filter.merge(
            GroupByItemSetFilter.create(any_properties_denylist=(GroupByItemProperty.METRIC,))
        )


@dataclass(frozen=True)
class EntityPattern(EntityLinkPattern):
    """Similar to EntityPathPattern but only matches entities.

    Analogous pattern for Entity() in the object builder naming scheme.
    """

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        spec_set = group_specs_by_type(candidate_specs)
        return super().match(spec_set.entity_specs)

    @staticmethod
    def from_call_parameter_set(entity_call_parameter_set: EntityCallParameterSet) -> EntityPattern:  # noqa: D102
        return EntityPattern(
            parameter_set=SpecPatternParameterSet.from_parameters(
                fields_to_compare=(
                    ParameterSetField.ELEMENT_NAME,
                    ParameterSetField.ENTITY_LINKS,
                ),
                element_name=entity_call_parameter_set.entity_reference.element_name,
                entity_links=entity_call_parameter_set.entity_path,
                descending=entity_call_parameter_set.descending,
            )
        )

    @property
    @override
    def element_pre_filter(self) -> GroupByItemSetFilter:
        return GroupByItemSetFilter.create(any_properties_denylist=(GroupByItemProperty.METRIC,))


@dataclass(frozen=True)
class GroupByMetricPattern(EntityLinkPattern):
    """A pattern that matches metrics using the group by specifications."""

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        spec_set = group_specs_by_type(candidate_specs)
        return super().match(spec_set.group_by_metric_specs)

    @staticmethod
    def from_call_parameter_set(  # noqa: D102
        metric_call_parameter_set: MetricCallParameterSet,
    ) -> GroupByMetricPattern:
        metric_time_group_bys = tuple(
            group_by_ref
            for group_by_ref in metric_call_parameter_set.group_by
            if group_by_ref.element_name == METRIC_TIME_ELEMENT_NAME
        )
        non_metric_time_group_bys = tuple(
            group_by_ref
            for group_by_ref in metric_call_parameter_set.group_by
            if group_by_ref.element_name != METRIC_TIME_ELEMENT_NAME
        )
        if len(metric_time_group_bys) > 1 or len(non_metric_time_group_bys) != 1:
            raise UnableToSatisfyQueryError(
                "Metric filters require exactly one non-metric_time group by item, with optional metric_time."
            )
        group_by = non_metric_time_group_bys[0]
        if group_by.time_granularity_name is not None:
            raise UnableToSatisfyQueryError("Metric filters only support metric_time as a time dimension in group_by.")
        metric_subquery_entity_links = group_by.entity_links + (EntityReference(group_by.element_name),)
        # Temp: we don't have a parameter to specify the join path from the outer query to the metric subquery,
        # so just use the last entity. Will need to add another param for that later.
        entity_links = metric_subquery_entity_links[-1:]
        return GroupByMetricPattern(
            parameter_set=SpecPatternParameterSet.from_parameters(
                fields_to_compare=(
                    ParameterSetField.ELEMENT_NAME,
                    ParameterSetField.ENTITY_LINKS,
                    ParameterSetField.METRIC_SUBQUERY_ENTITY_LINKS,
                ),
                element_name=metric_call_parameter_set.metric_reference.element_name,
                entity_links=entity_links,
                metric_subquery_entity_links=metric_subquery_entity_links,
                descending=metric_call_parameter_set.descending,
            )
        )
