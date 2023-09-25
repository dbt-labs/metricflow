from __future__ import annotations

from typing import Optional

from dbt_semantic_interfaces.call_parameter_sets import ParseWhereFilterException
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from dbt_semantic_interfaces.naming.keywords import DUNDER
from typing_extensions import override

from metricflow.specs.patterns.entity_path_pattern import EntityPathPattern, EntityPathPatternParameterSet
from metricflow.specs.patterns.spec_pattern import QueryItemNamingScheme, SpecPattern
from metricflow.specs.specs import InstanceSpecSet, InstanceSpecSetTransform, LinkableInstanceSpec


class PythonObjectNamingScheme(QueryItemNamingScheme[LinkableInstanceSpec]):
    """A naming scheme using Python object syntax like TimeDimension('metric_time', time_granularity_name='day')."""

    @override
    def input_str(self, instance_spec: LinkableInstanceSpec) -> Optional[str]:
        return _PythonObjectNameTransform().transform(instance_spec.as_spec_set)

    @override
    def output_column_name(self, instance_spec: LinkableInstanceSpec) -> Optional[str]:
        raise NotImplementedError("Using this naming scheme for naming output columns is not yet supported.")

    @override
    def spec_pattern(self, input_str: str) -> SpecPattern[LinkableInstanceSpec]:
        try:
            # TODO: Update when more appropriate parsing libraries are available.
            call_parameter_sets = PydanticWhereFilter(where_sql_template="{{ " + input_str + " }}").call_parameter_sets
        except ParseWhereFilterException as e:
            raise RuntimeError(f"A spec pattern can't be generated from the input string `{input_str}`") from e

        num_parameter_sets = (
            len(call_parameter_sets.dimension_call_parameter_sets)
            + len(call_parameter_sets.time_dimension_call_parameter_sets)
            + len(call_parameter_sets.entity_call_parameter_sets)
        )
        if num_parameter_sets != 1:
            raise RuntimeError(f"Did not find exactly 1 call parameter set. Got: {num_parameter_sets}")

        for dimension_call_parameter_set in call_parameter_sets.dimension_call_parameter_sets:
            return EntityPathPattern(
                EntityPathPatternParameterSet(
                    element_name=dimension_call_parameter_set.dimension_reference.element_name,
                    entity_links=dimension_call_parameter_set.entity_path,
                    time_granularity=None,
                    date_part=None,
                    input_string=input_str,
                    naming_scheme=self,
                )
            )

        for time_dimension_call_parameter_set in call_parameter_sets.time_dimension_call_parameter_sets:
            # TODO: Temporary heuristic check until date_part support is fully in.
            if input_str.find("date_part"):
                raise NotImplementedError("date_part support is blocked on appropriate parsing in DSI")
            return EntityPathPattern(
                EntityPathPatternParameterSet(
                    element_name=time_dimension_call_parameter_set.time_dimension_reference.element_name,
                    entity_links=time_dimension_call_parameter_set.entity_path,
                    time_granularity=time_dimension_call_parameter_set.time_granularity,
                    date_part=None,
                    input_string=input_str,
                    naming_scheme=self,
                )
            )

        for entity_call_parameter_set in call_parameter_sets.entity_call_parameter_sets:
            return EntityPathPattern(
                EntityPathPatternParameterSet(
                    element_name=entity_call_parameter_set.entity_reference.element_name,
                    entity_links=entity_call_parameter_set.entity_path,
                    time_granularity=None,
                    date_part=None,
                    input_string=input_str,
                    naming_scheme=self,
                )
            )

        raise RuntimeError("There should have been a return associated with one of the CallParameterSets.")

    @override
    def input_str_follows_scheme(self, input_str: str) -> bool:
        try:
            PydanticWhereFilter(where_sql_template="{{ " + input_str + " }}").call_parameter_sets,
            return True
        except ParseWhereFilterException:
            return False

    @property
    @override
    def input_str_description(self) -> str:
        return (
            "The input string should follow the conventions for specifying group by items in the Python object format."
        )


class _PythonObjectNameTransform(InstanceSpecSetTransform[str]):
    """Transforms specs into inputs following the Python object scheme.

    The input set should have exactly one group by item spec.
    """

    @override
    def transform(self, spec_set: InstanceSpecSet) -> str:
        assert len(spec_set.metric_specs) == 0
        assert len(spec_set.metadata_specs) == 0
        assert len(spec_set.measure_specs) == 0
        assert len(spec_set.entity_specs) + len(spec_set.dimension_specs) + len(spec_set.time_dimension_specs) == 1

        for instance_spec in spec_set.time_dimension_specs + spec_set.entity_specs + spec_set.dimension_specs:
            if len(instance_spec.entity_links) == 0:
                raise RuntimeError(
                    "The Python object naming scheme should have only been applied to specs with entity links."
                )

        for time_dimension_spec in spec_set.time_dimension_specs:
            primary_entity_name = time_dimension_spec.entity_links[-1].element_name
            other_entity_names = tuple(
                entity_link.element_name for entity_link in time_dimension_spec.entity_links[:-1]
            )
            initializer_parameters = [
                f"'{primary_entity_name}{DUNDER}{time_dimension_spec.element_name}'",
                f"time_granularity_name='{time_dimension_spec.time_granularity.value}')",
            ]
            if time_dimension_spec.date_part is not None:
                initializer_parameters.append(f"date_part_name='{time_dimension_spec.date_part.value}'")
            initializer_parameters.append(f"entity_path={str(other_entity_names)}")
            initializer_parameter_str = ", ".join(initializer_parameters)
            return f"TimeDimension({initializer_parameter_str})"

        raise RuntimeError(f"There should have been a return associated with one of the specs in {spec_set}")
