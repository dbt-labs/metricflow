from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    MetricCallParameterSet,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.references import EntityReference
from typing_extensions import override

from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
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

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        spec_set = group_specs_by_type(candidate_specs)
        filtered_specs: Sequence[LinkableInstanceSpec] = spec_set.dimension_specs + spec_set.time_dimension_specs
        return super().match(filtered_specs)

    @staticmethod
    def from_call_parameter_set(  # noqa: D102
        dimension_call_parameter_set: DimensionCallParameterSet,
    ) -> DimensionPattern:
        return DimensionPattern(
            parameter_set=SpecPatternParameterSet.from_parameters(
                fields_to_compare=(
                    ParameterSetField.ELEMENT_NAME,
                    ParameterSetField.ENTITY_LINKS,
                ),
                element_name=dimension_call_parameter_set.dimension_reference.element_name,
                entity_links=dimension_call_parameter_set.entity_path,
            )
        )

    @property
    @override
    def element_pre_filter(self) -> LinkableElementFilter:
        return super().element_pre_filter.merge(
            LinkableElementFilter(without_any_of=frozenset({LinkableElementProperty.METRIC}))
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

        if time_dimension_call_parameter_set.time_granularity_name is not None:
            fields_to_compare.append(ParameterSetField.TIME_GRANULARITY)

        return TimeDimensionPattern(
            parameter_set=SpecPatternParameterSet.from_parameters(
                fields_to_compare=tuple(fields_to_compare),
                element_name=time_dimension_call_parameter_set.time_dimension_reference.element_name,
                entity_links=time_dimension_call_parameter_set.entity_path,
                time_granularity_name=time_dimension_call_parameter_set.time_granularity_name,
                date_part=time_dimension_call_parameter_set.date_part,
            )
        )

    @property
    @override
    def element_pre_filter(self) -> LinkableElementFilter:
        return super().element_pre_filter.merge(
            LinkableElementFilter(without_any_of=frozenset({LinkableElementProperty.METRIC}))
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
            )
        )

    @property
    @override
    def element_pre_filter(self) -> LinkableElementFilter:
        return LinkableElementFilter(without_any_of=frozenset({LinkableElementProperty.METRIC}))


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
        # This looks hacky because the typing for the interface does not match the implementation, but that's temporary!
        # This will get a lot less hacky once we enable multiple entities and dimensions in the group by.
        if len(metric_call_parameter_set.group_by) != 1:
            raise RuntimeError(
                "Currently only one group by item is allowed for Metric filters. "
                "This should have been caught by validations."
            )
        group_by = metric_call_parameter_set.group_by[0]
        # custom_granularity_names is empty because we are not parsing any dimensions here with grain
        structured_name = StructuredLinkableSpecName.from_name(
            qualified_name=group_by.element_name, custom_granularity_names=()
        )
        metric_subquery_entity_links = tuple(
            EntityReference(entity_name)
            for entity_name in (structured_name.entity_link_names + (structured_name.element_name,))
        )
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
            )
        )
