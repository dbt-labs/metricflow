from __future__ import annotations

from typing import Optional

from dbt_semantic_interfaces.naming.keywords import DUNDER
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from typing_extensions import override

from metricflow.specs.patterns.entity_path_pattern import EntityPathPattern, EntityPathPatternParameterSet
from metricflow.specs.patterns.spec_pattern import QueryItemNamingScheme, SpecPattern
from metricflow.specs.specs import InstanceSpecSet, InstanceSpecSetTransform, LinkableInstanceSpec
from metricflow.time.date_part import DatePart


class DunderNamingScheme(QueryItemNamingScheme[LinkableInstanceSpec]):
    """A naming scheme that mirrors the behavior of StructuredLinkableSpecName.

    TODO: Replace StructuredLinkableSpecName with this.

    * See input_str_description().
    * The behavior in StructuredLinkableSpecName when a TimeDimensionSpec has a date part is nuanced. When a
      TimeDimensionSpec has a date_part, a column name can be formed. However, an input string cannot contain a
      date part.
    """

    @staticmethod
    def _date_part_suffix(date_part: DatePart) -> str:
        """Suffix used for names with a date_part."""
        return f"extract_{date_part.value}"

    @override
    def input_str(self, instance_spec: LinkableInstanceSpec) -> Optional[str]:
        spec_set = instance_spec.as_spec_set

        for time_dimension_spec in spec_set.time_dimension_specs:
            # From existing comment in StructuredLinkableSpecName:
            #
            # Dunder syntax not supported for querying date_part
            #
            if time_dimension_spec.date_part is not None:
                return None
        return self.output_column_name(instance_spec)

    @override
    def output_column_name(self, instance_spec: LinkableInstanceSpec) -> str:
        return _DunderNameTransform().transform(instance_spec.as_spec_set)

    @override
    def spec_pattern(self, input_str: str) -> SpecPattern[LinkableInstanceSpec]:
        if not self.input_str_follows_scheme(input_str):
            raise RuntimeError(f"`{input_str}` does not follow this scheme.")
        input_str_parts = input_str.split(DUNDER)

        # No dunder, e.g. "ds"
        if len(input_str_parts) == 1:
            return EntityPathPattern(
                parameter_set=EntityPathPatternParameterSet(
                    element_name=input_str_parts[0],
                    entity_links=(),
                    time_granularity=None,
                    date_part=None,
                    input_string=input_str,
                    naming_scheme=self,
                )
            )

        associated_granularity = None
        for granularity in TimeGranularity:
            if input_str_parts[-1] == granularity.value:
                associated_granularity = granularity

        # Has a time granularity
        if associated_granularity is not None:
            #  e.g. "ds__month"
            if len(input_str_parts) == 2:
                return EntityPathPattern(
                    parameter_set=EntityPathPatternParameterSet(
                        element_name=input_str_parts[0],
                        entity_links=(),
                        time_granularity=associated_granularity,
                        date_part=None,
                        input_string=input_str,
                        naming_scheme=self,
                    )
                )
            # e.g. "messages__ds__month"
            return EntityPathPattern(
                parameter_set=EntityPathPatternParameterSet(
                    element_name=input_str_parts[-2],
                    entity_links=tuple(EntityReference(entity_name) for entity_name in input_str_parts[:-2]),
                    time_granularity=associated_granularity,
                    date_part=None,
                    input_string=input_str,
                    naming_scheme=self,
                )
            )

        # e.g. "messages__ds"
        else:
            return EntityPathPattern(
                parameter_set=EntityPathPatternParameterSet(
                    element_name=input_str_parts[-1],
                    entity_links=tuple(EntityReference(entity_name) for entity_name in input_str_parts[:-1]),
                    time_granularity=None,
                    date_part=None,
                    input_string=input_str,
                    naming_scheme=self,
                )
            )

    @override
    def input_str_follows_scheme(self, input_str: str) -> bool:
        input_str_parts = input_str.split(DUNDER)

        for date_part in DatePart:
            if input_str_parts[-1] == DunderNamingScheme._date_part_suffix(date_part=date_part):
                # From existing message in StructuredLinkableSpecName: "Dunder syntax not supported for querying
                # date_part".
                return False

        return True

    @property
    @override
    def input_str_description(self) -> str:
        return (
            "The input string should be a sequence of strings consisting of the entity links, the name of the "
            "dimension or entity, and a time granularity (if applicable), joined by a double underscore. e.g. "
            "listing__user__country or metric_time__day."
        )


class _DunderNameTransform(InstanceSpecSetTransform[str]):
    """Transforms group by item specs into the appropriate string."""

    @override
    def transform(self, spec_set: InstanceSpecSet) -> str:
        assert len(spec_set.measure_specs) == 0
        assert len(spec_set.metric_specs) == 0
        assert len(spec_set.metadata_specs) == 0

        for time_dimension_spec in spec_set.time_dimension_specs:
            items = list(entity_link.element_name for entity_link in time_dimension_spec.entity_links) + [
                time_dimension_spec.element_name
            ]
            if time_dimension_spec.date_part is not None:
                items.append(DunderNamingScheme._date_part_suffix(date_part=time_dimension_spec.date_part))
            else:
                items.append(time_dimension_spec.time_granularity.value)
            return DUNDER.join(items)

        for other_group_by_item_specs in spec_set.entity_specs + spec_set.dimension_specs:
            items = list(entity_link.element_name for entity_link in other_group_by_item_specs.entity_links) + [
                other_group_by_item_specs.element_name
            ]
            return DUNDER.join(items)

        raise RuntimeError(f"Did not find any appropriate specs in {spec_set}")
