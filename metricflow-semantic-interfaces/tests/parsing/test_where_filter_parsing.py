"""Tests various where filter parsing conditions.

WhereFilter parsing operations can be fairly complex, as they must be able to accept input that is
either a bare string filter expression or some partially or fully deserialized filter object type.

In addition, due to the migration from WhereFilter to WhereFilterIntersection types, this tests the
various conversion operations we will need to perform on semantic manifests defined out in the world.

This module tests the various combinations we might encounter in the wild, with a particular focus
on inputs to parse_obj or parse_raw, as that is what the pydantic models will generally encounter.
"""
from __future__ import annotations

from typing import Tuple, Union

import pytest
from metricflow_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    MetricCallParameterSet,
    TimeDimensionCallParameterSet,
)
from metricflow_semantic_interfaces.implementations.base import HashableBaseModel
from metricflow_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
    PydanticWhereFilterIntersection,
)
from metricflow_semantic_interfaces.parsing.where_filter.jinja_object_parser import (
    JinjaObjectParser,
    QueryItemLocation,
)
from metricflow_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    LinkableElementReference,
    MetricReference,
    TimeDimensionReference,
)
from metricflow_semantic_interfaces.type_enums import DatePart, TimeGranularity

__BOOLEAN_EXPRESSION__ = "1 > 0"


class ModelWithWhereFilter(HashableBaseModel):
    """Defines a test model to allow for evaluation of different parsing modes for where filter expressions."""

    where_filter: PydanticWhereFilter


class ModelWithWhereFilterIntersection(HashableBaseModel):
    """Defines a test model to allow for evaluation of different parsing modes for where filter intersections.

    This has the same schema, apart from the filter type, as the ModelWithWhereFilter in order to allow for
    testing conversion from a WhereFilter to a WhereFilterIntersection.
    """

    where_filter: PydanticWhereFilterIntersection


def test_partially_deserialized_object_string_parsing() -> None:
    """Tests parsing a where filter specified as a string within partially deserialized json object."""
    obj = {"where_filter": __BOOLEAN_EXPRESSION__}

    parsed_model = ModelWithWhereFilter.parse_obj(obj)

    assert parsed_model.where_filter == PydanticWhereFilter(where_sql_template=__BOOLEAN_EXPRESSION__)


def test_partially_deserialized_object_parsing() -> None:
    """Tests parsing a where filter that was serialized and then json decoded, but not fully parsed."""
    obj = {"where_filter": {"where_sql_template": __BOOLEAN_EXPRESSION__}}

    parsed_model = ModelWithWhereFilter.parse_obj(obj)

    assert parsed_model.where_filter == PydanticWhereFilter(where_sql_template=__BOOLEAN_EXPRESSION__)


def test_injected_object_parsing() -> None:
    """Tests parsing where, for some reason, a PydanticWhereFilter has been injected into the object.

    This covers the (hopefully vanishingly rare) cases where some raw validator in a pydantic implementation
    is updating the input object to convert something to a PydanticWhereFilter.
    """
    obj = {"where_filter": PydanticWhereFilter(where_sql_template=__BOOLEAN_EXPRESSION__)}

    parsed_model = ModelWithWhereFilter.parse_obj(obj)

    assert parsed_model.where_filter == PydanticWhereFilter(where_sql_template=__BOOLEAN_EXPRESSION__)


def test_serialize_deserialize_operations() -> None:
    """Tests serializing and deserializing an object with a WhereFilter.

    This should cover the most common scenarios, where we need to parse a serialized SemanticManifest.
    """
    base_obj = ModelWithWhereFilter(where_filter=PydanticWhereFilter(where_sql_template=__BOOLEAN_EXPRESSION__))

    serialized = base_obj.json()
    deserialized = ModelWithWhereFilter.parse_raw(serialized)

    assert deserialized == base_obj


def test_conversion_from_partially_deserialized_where_filter_string() -> None:
    """Tests converting a partially deserialized ModelWithWhereFilter into a ModelWithWhereFilterIntersection.

    This covers the case where the input is still a bare string, such as might happen in a raw YAML read.
    """
    obj = {"where_filter": __BOOLEAN_EXPRESSION__}
    expected_conversion_output = PydanticWhereFilterIntersection(
        where_filters=[PydanticWhereFilter(where_sql_template=__BOOLEAN_EXPRESSION__)]
    )

    parsed_model = ModelWithWhereFilterIntersection.parse_obj(obj)

    assert parsed_model.where_filter == expected_conversion_output


def test_conversion_from_partially_deserialized_where_filter_object() -> None:
    """Tests converting a partially deserialized WhereFilter into a WhereFilterIntersection."""
    obj = {"where_filter": {"where_sql_template": __BOOLEAN_EXPRESSION__}}
    expected_conversion_output = PydanticWhereFilterIntersection(
        where_filters=[PydanticWhereFilter(where_sql_template=__BOOLEAN_EXPRESSION__)]
    )

    parsed_model = ModelWithWhereFilterIntersection.parse_obj(obj)

    assert parsed_model.where_filter == expected_conversion_output


def test_conversion_from_injected_where_filter_object() -> None:
    """Tests conversion from a PydanticWhereFilter instance, such as one inserted via a raw validator."""
    obj = {"where_filter": PydanticWhereFilter(where_sql_template=__BOOLEAN_EXPRESSION__)}
    expected_conversion_output = PydanticWhereFilterIntersection(
        where_filters=[PydanticWhereFilter(where_sql_template=__BOOLEAN_EXPRESSION__)]
    )

    parsed_model = ModelWithWhereFilterIntersection.parse_obj(obj)

    assert parsed_model.where_filter == expected_conversion_output


def test_where_filter_intersection_from_partially_deserialized_list_of_strings() -> None:
    """Tests parsing a PydanticWhereFilterIntersection when the input is a list of strings.

    This simulates handling YAML input, which may be a list or other sequence of filters.
    """
    obj = {"where_filter": [__BOOLEAN_EXPRESSION__, "0 < 1"]}
    expected_parsed_output = PydanticWhereFilterIntersection(
        where_filters=[
            PydanticWhereFilter(where_sql_template=__BOOLEAN_EXPRESSION__),
            PydanticWhereFilter(where_sql_template="0 < 1"),
        ]
    )

    parsed_model = ModelWithWhereFilterIntersection.parse_obj(obj)

    assert parsed_model.where_filter == expected_parsed_output


@pytest.mark.parametrize(
    "where",
    [
        "{{ TimeDimension('metric_time', 'YEAR', [], None, 'YEAR') }} > '2023-01-01'",
        "{{ TimeDimension(time_dimension_name='metric_time', time_granularity_name='YEAR', date_part_name='YEAR') }}"
        + "> '2023-01-01'",
    ],
)
def test_time_dimension_date_part(where: str) -> None:  # noqa
    param_sets = JinjaObjectParser.parse_call_parameter_sets(
        where, custom_granularity_names=(), query_item_location=QueryItemLocation.NON_ORDER_BY
    )
    assert len(param_sets.time_dimension_call_parameter_sets) == 1
    assert param_sets.time_dimension_call_parameter_sets[0].date_part == DatePart.YEAR


def test_dimension_date_part() -> None:  # noqa
    where = "{{ Dimension('metric_time').grain('DAY').date_part('YEAR') }} > '2023-01-01'"
    param_sets = JinjaObjectParser.parse_call_parameter_sets(
        where, custom_granularity_names=(), query_item_location=QueryItemLocation.NON_ORDER_BY
    )
    assert len(param_sets.time_dimension_call_parameter_sets) == 1
    assert param_sets.time_dimension_call_parameter_sets[0].date_part == DatePart.YEAR


@pytest.mark.parametrize(
    "where_and_expected_call_params",
    [
        (
            "{{ TimeDimension('metric_time__week') }} > '2023-01-01'",
            TimeDimensionCallParameterSet(
                time_dimension_reference=TimeDimensionReference("metric_time"),
                entity_path=(),
                time_granularity_name=TimeGranularity.WEEK.value,
            ),
        ),
        (
            "{{ TimeDimension('metric_time', time_granularity_name='week') }} > '2023-01-01'",
            TimeDimensionCallParameterSet(
                time_dimension_reference=TimeDimensionReference("metric_time"),
                entity_path=(),
                time_granularity_name=TimeGranularity.WEEK.value,
            ),
        ),
        (
            "{{ TimeDimension('metric_time__martian_week') }} > '2023-01-01'",
            TimeDimensionCallParameterSet(
                time_dimension_reference=TimeDimensionReference("metric_time"),
                entity_path=(),
                time_granularity_name="martian_week",
            ),
        ),
        (
            "{{ TimeDimension('metric_time', time_granularity_name='martian_week') }} > '2023-01-01'",
            TimeDimensionCallParameterSet(
                time_dimension_reference=TimeDimensionReference("metric_time"),
                entity_path=(),
                time_granularity_name="martian_week",
            ),
        ),
    ],
)
def test_time_dimension_grain(  # noqa
    where_and_expected_call_params: Tuple[str, Union[TimeDimensionCallParameterSet, DimensionCallParameterSet]],
) -> None:
    where, expected_call_params = where_and_expected_call_params
    param_sets = JinjaObjectParser.parse_call_parameter_sets(
        where, custom_granularity_names=("martian_week",), query_item_location=QueryItemLocation.NON_ORDER_BY
    )
    assert len(param_sets.time_dimension_call_parameter_sets) == 1
    assert param_sets.time_dimension_call_parameter_sets[0] == expected_call_params


def test_entity_without_primary_entity_prefix() -> None:  # noqa
    where = "{{ Entity('non_primary_entity') }} = '1'"
    param_sets = JinjaObjectParser.parse_call_parameter_sets(
        where, custom_granularity_names=(), query_item_location=QueryItemLocation.NON_ORDER_BY
    )
    assert len(param_sets.entity_call_parameter_sets) == 1
    assert param_sets.entity_call_parameter_sets[0] == EntityCallParameterSet(
        entity_path=(),
        entity_reference=EntityReference(element_name="non_primary_entity"),
    )


def test_entity() -> None:  # noqa
    where = "{{ Entity('entity_1__entity_2', entity_path=['entity_0']) }} = '1'"
    param_sets = JinjaObjectParser.parse_call_parameter_sets(
        where, custom_granularity_names=(), query_item_location=QueryItemLocation.NON_ORDER_BY
    )
    assert len(param_sets.entity_call_parameter_sets) == 1
    assert param_sets.entity_call_parameter_sets[0] == EntityCallParameterSet(
        entity_path=(
            EntityReference(element_name="entity_0"),
            EntityReference(element_name="entity_1"),
        ),
        entity_reference=EntityReference(element_name="entity_2"),
    )


def test_metric() -> None:  # noqa
    where = "{{ Metric('metric', group_by=['dimension']) }} = 10"
    param_sets = JinjaObjectParser.parse_call_parameter_sets(
        where, custom_granularity_names=(), query_item_location=QueryItemLocation.NON_ORDER_BY
    )
    assert len(param_sets.metric_call_parameter_sets) == 1
    assert param_sets.metric_call_parameter_sets[0] == MetricCallParameterSet(
        group_by=(LinkableElementReference(element_name="dimension"),),
        metric_reference=MetricReference(element_name="metric"),
    )

    # Without kwarg syntax
    where = "{{ Metric('metric', ['dimension']) }} = 10"
    param_sets = JinjaObjectParser.parse_call_parameter_sets(
        where, custom_granularity_names=(), query_item_location=QueryItemLocation.NON_ORDER_BY
    )
    assert len(param_sets.metric_call_parameter_sets) == 1
    assert param_sets.metric_call_parameter_sets[0] == MetricCallParameterSet(
        group_by=(LinkableElementReference(element_name="dimension"),),
        metric_reference=MetricReference(element_name="metric"),
    )


def test_order_by_params() -> None:  # noqa
    input_str = "{{ Metric('metric').descending(True) }} = 10"
    param_sets = JinjaObjectParser.parse_call_parameter_sets(
        input_str, custom_granularity_names=(), query_item_location=QueryItemLocation.ORDER_BY
    )
    assert len(param_sets.metric_call_parameter_sets) == 1
    assert param_sets.metric_call_parameter_sets[0] == MetricCallParameterSet(
        metric_reference=MetricReference(element_name="metric"), descending=True
    )

    input_str = "{{ Dimension('entity__stuff').descending(True) }} = 10"
    param_sets = JinjaObjectParser.parse_call_parameter_sets(
        input_str, custom_granularity_names=(), query_item_location=QueryItemLocation.ORDER_BY
    )
    assert len(param_sets.dimension_call_parameter_sets) == 1
    assert param_sets.dimension_call_parameter_sets[0] == DimensionCallParameterSet(
        dimension_reference=DimensionReference(element_name="stuff"),
        entity_path=(EntityReference(element_name="entity"),),
        descending=True,
    )

    input_str = "{{ TimeDimension('entity__time_stuff').descending(True) }} = 10"
    param_sets = JinjaObjectParser.parse_call_parameter_sets(
        input_str, custom_granularity_names=(), query_item_location=QueryItemLocation.ORDER_BY
    )
    assert len(param_sets.time_dimension_call_parameter_sets) == 1
    assert param_sets.time_dimension_call_parameter_sets[0] == TimeDimensionCallParameterSet(
        time_dimension_reference=TimeDimensionReference(element_name="time_stuff"),
        entity_path=(EntityReference(element_name="entity"),),
        descending=True,
    )
