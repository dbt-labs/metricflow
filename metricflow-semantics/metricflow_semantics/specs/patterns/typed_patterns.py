from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    MetricCallParameterSet,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums.date_part import DatePart
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
    def from_call_parameter_set(metric_call_parameter_set: MetricCallParameterSet) -> GroupByMetricPattern:
        """
        Builds a GroupByMetricPattern that can handle multiple group_by items, including the special case
        of 'metric_time' (treated as a time dimension / virtual dimension for the metric).

        Implementation notes:
        - We accumulate all group_by items into 'metric_subquery_entity_links' for now.
        - The 'entity_links' used in the top-level pattern is set to the last group_by item by default.
          (This preserves backward compatibility, while allowing multiple group_by items.)
        - If any group_by item is 'metric_time', we allow that to be included as though it is a dimension or
          an entity reference with a special name. In real usage, 'metric_time' is recognized later as
          a time dimension. This logic simply ensures that the pattern can match the final resolved
          `GroupByMetricSpec`.
        """
        metric_subquery_entity_links_list = []
        all_group_by_refs = metric_call_parameter_set.group_by

        # If there are no group_by items, we proceed with empty links.
        if not all_group_by_refs:
            return GroupByMetricPattern(
                parameter_set=SpecPatternParameterSet.from_parameters(
                    fields_to_compare=(
                        ParameterSetField.ELEMENT_NAME,
                        ParameterSetField.ENTITY_LINKS,
                        ParameterSetField.METRIC_SUBQUERY_ENTITY_LINKS,
                    ),
                    element_name=metric_call_parameter_set.metric_reference.element_name,
                    entity_links=(),
                    metric_subquery_entity_links=(),
                )
            )

        # Convert each item in group_by to something we can store in metric_subquery_entity_links.
        # We'll parse them via StructuredLinkableSpecName (handles "listing__user" style references)
        # but treat "metric_time" specially if found.
        for group_by_ref in all_group_by_refs:
            if group_by_ref.element_name == "metric_time":
                # For minimal changes, store 'metric_time' as an entity reference with no link names.
                metric_subquery_entity_links_list.append(EntityReference("metric_time"))
            else:
                structured_name = StructuredLinkableSpecName.from_name(
                    qualified_name=group_by_ref.element_name,
                    custom_granularity_names=(),
                )
                # E.g. "listing__account_id" => entity_link_names=["listing"], element_name="account_id"
                # So we place them in one linear chain: ("listing","account_id"), etc.
                subquery_entity_names = list(structured_name.entity_link_names) + [structured_name.element_name]
                for entity_name in subquery_entity_names:
                    metric_subquery_entity_links_list.append(EntityReference(entity_name))

        # The last item in metric_subquery_entity_links_list is used as the top-level entity link
        # for the outer metric reference. This is the same behavior as prior to multi-group-by support.
        if metric_subquery_entity_links_list:
            entity_links = (metric_subquery_entity_links_list[-1],)
        else:
            entity_links = ()

        return GroupByMetricPattern(
            parameter_set=SpecPatternParameterSet.from_parameters(
                fields_to_compare=(
                    ParameterSetField.ELEMENT_NAME,
                    ParameterSetField.ENTITY_LINKS,
                    ParameterSetField.METRIC_SUBQUERY_ENTITY_LINKS,
                ),
                element_name=metric_call_parameter_set.metric_reference.element_name,
                entity_links=entity_links,
                metric_subquery_entity_links=tuple(metric_subquery_entity_links_list),
            )
        )
