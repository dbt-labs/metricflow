from __future__ import annotations

import logging
import re
from typing import Optional

from dbt_semantic_interfaces.call_parameter_sets import (
    ParseWhereFilterException,
)
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from dbt_semantic_interfaces.parsing.where_filter.where_filter_parser import WhereFilterParser
from dbt_semantic_interfaces.references import EntityReference
from typing_extensions import override

from metricflow_semantics.errors.error_classes import InvalidQuerySyntax
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.naming.naming_scheme import QueryItemNamingScheme
from metricflow_semantics.naming.object_builder_str import ObjectBuilderNameConverter
from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.specs.patterns.entity_link_pattern import (
    EntityLinkPattern,
    ParameterSetField,
    SpecPatternParameterSet,
)
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern
from metricflow_semantics.specs.patterns.typed_patterns import DimensionPattern, TimeDimensionPattern

logger = logging.getLogger(__name__)


class ObjectBuilderNamingScheme(QueryItemNamingScheme):
    """A naming scheme using a builder syntax like Dimension('metric_time').grain('day')."""

    _NAME_REGEX = re.compile(r"\A(Dimension|TimeDimension|Entity|Metric)\(.*\)\Z")

    @override
    def input_str(self, instance_spec: InstanceSpec) -> Optional[str]:
        return ObjectBuilderNameConverter.input_str_from_spec(instance_spec)

    @override
    def spec_pattern(self, input_str: str, semantic_manifest_lookup: SemanticManifestLookup) -> SpecPattern:
        if not self.input_str_follows_scheme(input_str, semantic_manifest_lookup=semantic_manifest_lookup):
            raise InvalidQuerySyntax(
                f"The specified input {repr(input_str)} does not match the input described by the object builder "
                f"pattern."
            )
        try:
            # TODO: Update when more appropriate parsing libraries are available.
            call_parameter_sets = PydanticWhereFilter(where_sql_template="{{ " + input_str + " }}").call_parameter_sets(
                custom_granularity_names=semantic_manifest_lookup.semantic_model_lookup.custom_granularity_names
            )
        except ParseWhereFilterException as e:
            raise ValueError(f"A spec pattern can't be generated from the input string {repr(input_str)}") from e

        num_parameter_sets = (
            len(call_parameter_sets.dimension_call_parameter_sets)
            + len(call_parameter_sets.time_dimension_call_parameter_sets)
            + len(call_parameter_sets.entity_call_parameter_sets)
            + len(call_parameter_sets.metric_call_parameter_sets)
        )
        if num_parameter_sets != 1:
            raise ValueError(f"Did not find exactly 1 call parameter set. Got: {num_parameter_sets}")

        for dimension_call_parameter_set in call_parameter_sets.dimension_call_parameter_sets:
            return DimensionPattern(
                SpecPatternParameterSet.from_parameters(
                    element_name=dimension_call_parameter_set.dimension_reference.element_name,
                    entity_links=dimension_call_parameter_set.entity_path,
                    fields_to_compare=(
                        ParameterSetField.ELEMENT_NAME,
                        ParameterSetField.ENTITY_LINKS,
                        ParameterSetField.DATE_PART,
                    ),
                )
            )

        for time_dimension_call_parameter_set in call_parameter_sets.time_dimension_call_parameter_sets:
            return TimeDimensionPattern(
                SpecPatternParameterSet.from_parameters(
                    element_name=time_dimension_call_parameter_set.time_dimension_reference.element_name,
                    entity_links=time_dimension_call_parameter_set.entity_path,
                    time_granularity_name=time_dimension_call_parameter_set.time_granularity_name,
                    date_part=time_dimension_call_parameter_set.date_part,
                    fields_to_compare=TimeDimensionPattern.get_fields_to_compare(
                        time_granularity_name=time_dimension_call_parameter_set.time_granularity_name,
                        date_part=time_dimension_call_parameter_set.date_part,
                    ),
                )
            )

        for entity_call_parameter_set in call_parameter_sets.entity_call_parameter_sets:
            return EntityLinkPattern(
                SpecPatternParameterSet.from_parameters(
                    element_name=entity_call_parameter_set.entity_reference.element_name,
                    entity_links=entity_call_parameter_set.entity_path,
                    fields_to_compare=(
                        ParameterSetField.ELEMENT_NAME,
                        ParameterSetField.ENTITY_LINKS,
                    ),
                )
            )

        for metric_call_parameter_set in call_parameter_sets.metric_call_parameter_sets:
            return EntityLinkPattern(
                SpecPatternParameterSet.from_parameters(
                    element_name=metric_call_parameter_set.metric_reference.element_name,
                    entity_links=tuple(
                        EntityReference(element_name=group_by_ref.element_name)
                        for group_by_ref in metric_call_parameter_set.group_by
                    ),
                    fields_to_compare=(
                        ParameterSetField.ELEMENT_NAME,
                        ParameterSetField.ENTITY_LINKS,
                    ),
                )
            )

        raise RuntimeError("There should have been a return associated with one of the CallParameterSets.")

    @override
    def input_str_follows_scheme(self, input_str: str, semantic_manifest_lookup: SemanticManifestLookup) -> bool:
        if ObjectBuilderNamingScheme._NAME_REGEX.match(input_str) is None:
            return False
        try:
            call_parameter_sets = WhereFilterParser.parse_call_parameter_sets(
                where_sql_template="{{ " + input_str + " }}",
                custom_granularity_names=semantic_manifest_lookup.semantic_model_lookup.custom_granularity_names,
            )
            return_value = (
                len(call_parameter_sets.dimension_call_parameter_sets)
                + len(call_parameter_sets.time_dimension_call_parameter_sets)
                + len(call_parameter_sets.entity_call_parameter_sets)
                + len(call_parameter_sets.metric_call_parameter_sets)
            ) == 1
            return return_value
        except ParseWhereFilterException:
            return False

    @override
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id()={hex(id(self))})"
