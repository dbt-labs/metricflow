from __future__ import annotations

import re
from typing import Optional, Sequence, Tuple

from dbt_semantic_interfaces.naming.keywords import DUNDER
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from typing_extensions import override

from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.naming.naming_scheme import QueryItemNamingScheme
from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.specs.patterns.entity_link_pattern import (
    EntityLinkPattern,
    ParameterSetField,
    SpecPatternParameterSet,
)
from metricflow_semantics.specs.spec_set import InstanceSpecSet, InstanceSpecSetTransform, group_spec_by_type


class DunderNamingScheme(QueryItemNamingScheme):
    """A naming scheme using the dundered name syntax.

    TODO: Consolidate with StructuredLinkableSpecName.
    """

    _INPUT_REGEX = re.compile(r"\A[a-z]([a-z0-9_])*[a-z0-9]\Z")

    @staticmethod
    def date_part_suffix(date_part: DatePart) -> str:
        """Suffix used for names with a date_part."""
        return f"extract_{date_part.value}"

    @override
    def input_str(self, instance_spec: InstanceSpec) -> Optional[str]:
        spec_set = group_spec_by_type(instance_spec)

        for time_dimension_spec in spec_set.time_dimension_specs:
            # From existing comment in StructuredLinkableSpecName:
            #
            # Dunder syntax not supported for querying date_part
            #
            if time_dimension_spec.date_part is not None:
                return None
        names = _DunderNameTransform().transform(spec_set)
        if len(names) != 1:
            raise RuntimeError(f"Did not get 1 name for {instance_spec}. Got {names}")

        return names[0]

    @override
    def spec_pattern(self, input_str: str, semantic_manifest_lookup: SemanticManifestLookup) -> EntityLinkPattern:
        if not self.input_str_follows_scheme(input_str, semantic_manifest_lookup=semantic_manifest_lookup):
            raise ValueError(f"{repr(input_str)} does not follow this scheme.")

        input_str = input_str.lower()

        input_str_parts = input_str.split(DUNDER)
        fields_to_compare: Tuple[ParameterSetField, ...] = (
            ParameterSetField.ELEMENT_NAME,
            ParameterSetField.ENTITY_LINKS,
            ParameterSetField.DATE_PART,
        )

        # No dunder, e.g. "ds"
        if len(input_str_parts) == 1:
            return EntityLinkPattern(
                parameter_set=SpecPatternParameterSet.from_parameters(
                    element_name=input_str_parts[0],
                    entity_links=(),
                    time_granularity_name=None,
                    date_part=None,
                    fields_to_compare=tuple(fields_to_compare),
                )
            )

        # At this point, len(input_str_parts) >= 2
        valid_granularity_names = {granularity.value for granularity in TimeGranularity}.union(
            set(semantic_manifest_lookup.custom_granularities.keys())
        )
        suffix = input_str_parts[-1]
        time_granularity_name = suffix if suffix in valid_granularity_names else None
        # Has a time grain specified.
        if time_granularity_name is not None:
            fields_to_compare = fields_to_compare + (ParameterSetField.TIME_GRANULARITY,)
            #  e.g. "ds__month"
            if len(input_str_parts) == 2:
                return EntityLinkPattern(
                    parameter_set=SpecPatternParameterSet.from_parameters(
                        element_name=input_str_parts[0],
                        entity_links=(),
                        time_granularity_name=time_granularity_name,
                        date_part=None,
                        fields_to_compare=fields_to_compare,
                    )
                )
            # e.g. "messages__ds__month"
            return EntityLinkPattern(
                parameter_set=SpecPatternParameterSet.from_parameters(
                    element_name=input_str_parts[-2],
                    entity_links=tuple(EntityReference(entity_name) for entity_name in input_str_parts[:-2]),
                    time_granularity_name=time_granularity_name,
                    date_part=None,
                    fields_to_compare=fields_to_compare,
                )
            )

        # e.g. "messages__ds"
        return EntityLinkPattern(
            parameter_set=SpecPatternParameterSet.from_parameters(
                element_name=suffix,
                entity_links=tuple(EntityReference(entity_name) for entity_name in input_str_parts[:-1]),
                time_granularity_name=None,
                date_part=None,
                fields_to_compare=fields_to_compare,
            )
        )

    @override
    def input_str_follows_scheme(self, input_str: str, semantic_manifest_lookup: SemanticManifestLookup) -> bool:
        # This naming scheme is case-insensitive.
        input_str = input_str.lower()
        if DunderNamingScheme._INPUT_REGEX.match(input_str) is None:
            return False

        input_str_parts = input_str.split(DUNDER)

        for date_part in DatePart:
            # TODO: We don't enforce uniqueness against date part names. Could be blocking valid dimension names.
            if input_str_parts[-1] == DunderNamingScheme.date_part_suffix(date_part=date_part):
                # From existing message in StructuredLinkableSpecName: "Dunder syntax not supported for querying
                # date_part".
                return False

        return True

    @override
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id()={hex(id(self))})"


class _DunderNameTransform(InstanceSpecSetTransform[Sequence[str]]):
    """Transforms group-by-item spec into the dundered name."""

    @override
    def transform(self, spec_set: InstanceSpecSet) -> Sequence[str]:
        names_to_return = []

        for time_dimension_spec in spec_set.time_dimension_specs:
            items = list(entity_link.element_name for entity_link in time_dimension_spec.entity_links) + [
                time_dimension_spec.element_name
            ]
            if time_dimension_spec.date_part is not None:
                items.append(DunderNamingScheme.date_part_suffix(date_part=time_dimension_spec.date_part))
            else:
                items.append(time_dimension_spec.time_granularity.name)
            names_to_return.append(DUNDER.join(items))

        for other_group_by_item_specs in (
            spec_set.entity_specs + spec_set.dimension_specs + spec_set.group_by_metric_specs
        ):
            items = list(entity_link.element_name for entity_link in other_group_by_item_specs.entity_links) + [
                other_group_by_item_specs.element_name
            ]
            names_to_return.append(DUNDER.join(items))

        return sorted(names_to_return)
