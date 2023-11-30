from __future__ import annotations

import logging
import re
from typing import Optional, Sequence

from dbt_semantic_interfaces.call_parameter_sets import ParseWhereFilterException
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from dbt_semantic_interfaces.naming.keywords import DUNDER
from dbt_semantic_interfaces.parsing.where_filter.where_filter_parser import WhereFilterParser
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from typing_extensions import override

from metricflow.naming.naming_scheme import QueryItemNamingScheme
from metricflow.specs.patterns.entity_link_pattern import (
    EntityLinkPattern,
    EntityLinkPatternParameterSet,
    ParameterSetField,
)
from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.patterns.typed_patterns import DimensionPattern, TimeDimensionPattern
from metricflow.specs.specs import (
    InstanceSpec,
    InstanceSpecSet,
    InstanceSpecSetTransform,
)

logger = logging.getLogger(__name__)


class ObjectBuilderNamingScheme(QueryItemNamingScheme):
    """A naming scheme using a builder syntax like Dimension('metric_time').grain('day')."""

    _NAME_REGEX = re.compile(r"\A(Dimension|TimeDimension|Entity)\(.*\)\Z")

    @override
    def input_str(self, instance_spec: InstanceSpec) -> Optional[str]:
        names = _ObjectBuilderNameTransform().transform(InstanceSpecSet.from_specs((instance_spec,)))

        if len(names) != 1:
            raise RuntimeError(f"Did not get exactly 1 name from {instance_spec}. Got {names}")

        return names[0]

    @override
    def spec_pattern(self, input_str: str) -> SpecPattern:
        if not self.input_str_follows_scheme(input_str):
            raise ValueError(
                f"The specified input {repr(input_str)} does not match the input described by the object builder "
                f"pattern."
            )
        try:
            # TODO: Update when more appropriate parsing libraries are available.
            call_parameter_sets = PydanticWhereFilter(where_sql_template="{{ " + input_str + " }}").call_parameter_sets
        except ParseWhereFilterException as e:
            raise ValueError(f"A spec pattern can't be generated from the input string {repr(input_str)}") from e

        num_parameter_sets = (
            len(call_parameter_sets.dimension_call_parameter_sets)
            + len(call_parameter_sets.time_dimension_call_parameter_sets)
            + len(call_parameter_sets.entity_call_parameter_sets)
        )
        if num_parameter_sets != 1:
            raise ValueError(f"Did not find exactly 1 call parameter set. Got: {num_parameter_sets}")

        for dimension_call_parameter_set in call_parameter_sets.dimension_call_parameter_sets:
            return DimensionPattern(
                EntityLinkPatternParameterSet.from_parameters(
                    element_name=dimension_call_parameter_set.dimension_reference.element_name,
                    entity_links=dimension_call_parameter_set.entity_path,
                    time_granularity=None,
                    date_part=None,
                    fields_to_compare=(
                        ParameterSetField.ELEMENT_NAME,
                        ParameterSetField.ENTITY_LINKS,
                        ParameterSetField.DATE_PART,
                    ),
                )
            )

        for time_dimension_call_parameter_set in call_parameter_sets.time_dimension_call_parameter_sets:
            fields_to_compare = [
                ParameterSetField.ELEMENT_NAME,
                ParameterSetField.ENTITY_LINKS,
                ParameterSetField.DATE_PART,
            ]

            if time_dimension_call_parameter_set.time_granularity is not None:
                fields_to_compare.append(ParameterSetField.TIME_GRANULARITY)

            return TimeDimensionPattern(
                EntityLinkPatternParameterSet.from_parameters(
                    element_name=time_dimension_call_parameter_set.time_dimension_reference.element_name,
                    entity_links=time_dimension_call_parameter_set.entity_path,
                    time_granularity=time_dimension_call_parameter_set.time_granularity,
                    date_part=time_dimension_call_parameter_set.date_part,
                    fields_to_compare=tuple(fields_to_compare),
                )
            )

        for entity_call_parameter_set in call_parameter_sets.entity_call_parameter_sets:
            return EntityLinkPattern(
                EntityLinkPatternParameterSet.from_parameters(
                    element_name=entity_call_parameter_set.entity_reference.element_name,
                    entity_links=entity_call_parameter_set.entity_path,
                    time_granularity=None,
                    date_part=None,
                    fields_to_compare=(
                        ParameterSetField.ELEMENT_NAME,
                        ParameterSetField.ENTITY_LINKS,
                    ),
                )
            )

        raise RuntimeError("There should have been a return associated with one of the CallParameterSets.")

    @override
    def input_str_follows_scheme(self, input_str: str) -> bool:
        if ObjectBuilderNamingScheme._NAME_REGEX.match(input_str) is None:
            return False
        try:
            call_parameter_sets = WhereFilterParser.parse_call_parameter_sets("{{ " + input_str + " }}")
            return_value = (
                len(call_parameter_sets.dimension_call_parameter_sets)
                + len(call_parameter_sets.time_dimension_call_parameter_sets)
                + len(call_parameter_sets.entity_call_parameter_sets)
            ) == 1
            return return_value
        except ParseWhereFilterException:
            return False

    @override
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id()={hex(id(self))})"


class _ObjectBuilderNameTransform(InstanceSpecSetTransform[Sequence[str]]):
    """Transforms specs into strings following the object builder scheme."""

    @staticmethod
    def _get_initializer_parameter_str(
        element_name: str,
        entity_links: Sequence[EntityReference],
        time_granularity: Optional[TimeGranularity],
        date_part: Optional[DatePart],
    ) -> str:
        """Return the parameters that should go in the initializer.

        e.g. `'user__country', time_granularity_name='month'`
        """
        initializer_parameters = []
        entity_link_names = list(entity_link.element_name for entity_link in entity_links)
        if len(entity_link_names) > 0:
            initializer_parameters.append(repr(entity_link_names[-1] + DUNDER + element_name))
        else:
            initializer_parameters.append(repr(element_name))
        if time_granularity is not None:
            initializer_parameters.append(
                f"'{time_granularity.value}'",
            )
        if date_part is not None:
            initializer_parameters.append(f"date_part_name={repr(date_part.value)}")
        if len(entity_link_names) > 1:
            initializer_parameters.append(f"entity_path={repr(entity_link_names[:-1])}")

        return ", ".join(initializer_parameters)

    @override
    def transform(self, spec_set: InstanceSpecSet) -> Sequence[str]:
        assert len(spec_set.entity_specs) + len(spec_set.dimension_specs) + len(spec_set.time_dimension_specs) == 1

        names_to_return = []

        for entity_spec in spec_set.entity_specs:
            initializer_parameter_str = _ObjectBuilderNameTransform._get_initializer_parameter_str(
                element_name=entity_spec.element_name,
                entity_links=entity_spec.entity_links,
                time_granularity=None,
                date_part=None,
            )
            names_to_return.append(f"Entity({initializer_parameter_str})")

        for dimension_spec in spec_set.dimension_specs:
            initializer_parameter_str = _ObjectBuilderNameTransform._get_initializer_parameter_str(
                element_name=dimension_spec.element_name,
                entity_links=dimension_spec.entity_links,
                time_granularity=None,
                date_part=None,
            )
            names_to_return.append(f"Dimension({initializer_parameter_str})")

        for time_dimension_spec in spec_set.time_dimension_specs:
            initializer_parameter_str = _ObjectBuilderNameTransform._get_initializer_parameter_str(
                element_name=time_dimension_spec.element_name,
                entity_links=time_dimension_spec.entity_links,
                time_granularity=time_dimension_spec.time_granularity,
                date_part=time_dimension_spec.date_part,
            )
            names_to_return.append(f"TimeDimension({initializer_parameter_str})")

        return names_to_return
