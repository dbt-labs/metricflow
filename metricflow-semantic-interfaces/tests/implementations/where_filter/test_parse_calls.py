from __future__ import annotations

import logging

import pytest
from metricflow_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    JinjaCallParameterSets,
    MetricCallParameterSet,
    ParseJinjaObjectException,
    TimeDimensionCallParameterSet,
)
from metricflow_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
    PydanticWhereFilterIntersection,
)
from metricflow_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    LinkableElementReference,
    MetricReference,
    TimeDimensionReference,
)
from metricflow_semantic_interfaces.type_enums import TimeGranularity

logger = logging.getLogger(__name__)


def test_extract_dimension_call_parameter_sets() -> None:  # noqa: D103
    parse_result = PydanticWhereFilter(
        where_sql_template=(
            """\
                {{ Dimension('booking__is_instant') }} \
                AND {{ Dimension('user__country', entity_path=['listing']) }} == 'US'\
                """
        )
    ).call_parameter_sets(custom_granularity_names=())

    assert parse_result == JinjaCallParameterSets(
        dimension_call_parameter_sets=(
            DimensionCallParameterSet(
                dimension_reference=DimensionReference(element_name="is_instant"),
                entity_path=(EntityReference("booking"),),
            ),
            DimensionCallParameterSet(
                dimension_reference=DimensionReference(element_name="country"),
                entity_path=(
                    EntityReference("listing"),
                    EntityReference("user"),
                ),
            ),
        ),
        entity_call_parameter_sets=(),
    )


def test_extract_dimension_with_grain_call_parameter_sets() -> None:  # noqa: D103
    parse_result = PydanticWhereFilter(
        where_sql_template=(
            """
                {{ Dimension('metric_time').grain('WEEK') }} > 2023-09-18
            """
        )
    ).call_parameter_sets(custom_granularity_names=())

    assert parse_result == JinjaCallParameterSets(
        dimension_call_parameter_sets=(),
        time_dimension_call_parameter_sets=(
            TimeDimensionCallParameterSet(
                entity_path=(),
                time_dimension_reference=TimeDimensionReference(element_name="metric_time"),
                time_granularity_name=TimeGranularity.WEEK.value,
            ),
        ),
        entity_call_parameter_sets=(),
    )


def test_extract_time_dimension_call_parameter_sets() -> None:  # noqa: D103
    parse_result = PydanticWhereFilter(
        where_sql_template=(
            """{{ TimeDimension('user__created_at', 'month', entity_path=['listing']) }} = '2020-01-01'"""
        )
    ).call_parameter_sets(custom_granularity_names=())

    assert parse_result == JinjaCallParameterSets(
        time_dimension_call_parameter_sets=(
            TimeDimensionCallParameterSet(
                time_dimension_reference=TimeDimensionReference(element_name="created_at"),
                entity_path=(
                    EntityReference("listing"),
                    EntityReference("user"),
                ),
                time_granularity_name=TimeGranularity.MONTH.value,
            ),
        )
    )

    parse_result = PydanticWhereFilter(
        where_sql_template=(
            """{{ TimeDimension('user__created_at__month', entity_path=['listing']) }} = '2020-01-01'"""
        )
    ).call_parameter_sets(custom_granularity_names=())

    assert parse_result == JinjaCallParameterSets(
        time_dimension_call_parameter_sets=(
            TimeDimensionCallParameterSet(
                time_dimension_reference=TimeDimensionReference(element_name="created_at"),
                entity_path=(
                    EntityReference("listing"),
                    EntityReference("user"),
                ),
                time_granularity_name=TimeGranularity.MONTH.value,
            ),
        )
    )


def test_extract_metric_time_dimension_call_parameter_sets() -> None:  # noqa: D103
    parse_result = PydanticWhereFilter(
        where_sql_template="""{{ TimeDimension('metric_time', 'month') }} = '2020-01-01'"""
    ).call_parameter_sets(custom_granularity_names=())

    assert parse_result == JinjaCallParameterSets(
        time_dimension_call_parameter_sets=(
            TimeDimensionCallParameterSet(
                time_dimension_reference=TimeDimensionReference(element_name="metric_time"),
                entity_path=(),
                time_granularity_name=TimeGranularity.MONTH.value,
            ),
        )
    )


def test_extract_entity_call_parameter_sets() -> None:  # noqa: D103
    parse_result = PydanticWhereFilter(
        where_sql_template=(
            """{{ Entity('listing') }} AND {{ Entity('user', entity_path=['listing']) }} == 'TEST_USER_ID'"""
        )
    ).call_parameter_sets(custom_granularity_names=())

    assert parse_result == JinjaCallParameterSets(
        dimension_call_parameter_sets=(),
        entity_call_parameter_sets=(
            EntityCallParameterSet(
                entity_path=(),
                entity_reference=EntityReference("listing"),
            ),
            EntityCallParameterSet(
                entity_path=(EntityReference("listing"),),
                entity_reference=EntityReference("user"),
            ),
        ),
    )


def test_extract_metric_call_parameter_sets() -> None:  # noqa: D103
    parse_result = PydanticWhereFilter(
        where_sql_template=("{{ Metric('bookings', group_by=['listing']) }} > 2")
    ).call_parameter_sets(custom_granularity_names=())

    assert parse_result == JinjaCallParameterSets(
        dimension_call_parameter_sets=(),
        entity_call_parameter_sets=(),
        metric_call_parameter_sets=(
            MetricCallParameterSet(
                metric_reference=MetricReference("bookings"),
                group_by=(LinkableElementReference("listing"),),
            ),
        ),
    )

    parse_result = PydanticWhereFilter(
        where_sql_template=("{{ Metric('bookings', group_by=['listing', 'metric_time']) }} > 2")
    ).call_parameter_sets(custom_granularity_names=())

    assert parse_result == JinjaCallParameterSets(
        dimension_call_parameter_sets=(),
        entity_call_parameter_sets=(),
        metric_call_parameter_sets=(
            MetricCallParameterSet(
                metric_reference=MetricReference("bookings"),
                group_by=(LinkableElementReference("listing"), LinkableElementReference("metric_time")),
            ),
        ),
    )

    with pytest.raises(ParseJinjaObjectException):
        PydanticWhereFilter(where_sql_template=("{{ Metric('bookings') }} > 2")).call_parameter_sets(
            custom_granularity_names=()
        )


def test_invalid_entity_name_error() -> None:
    """Test to ensure we throw an error if an entity name is invalid."""
    bad_entity_filter = PydanticWhereFilter(where_sql_template="{{ Entity('is_food_order__day' )}}")

    with pytest.raises(ParseJinjaObjectException, match="Name is in an incorrect format"):
        bad_entity_filter.call_parameter_sets(custom_granularity_names=())


def test_where_filter_interesection_extract_call_parameter_sets() -> None:
    """Tests the collection of call parameter sets for a set of where filters."""
    time_filter = PydanticWhereFilter(
        where_sql_template=("""{{ TimeDimension('metric_time', 'month') }} = '2020-01-01'""")
    )
    entity_filter = PydanticWhereFilter(
        where_sql_template=(
            """{{ Entity('listing') }} AND {{ Entity('user', entity_path=['listing']) }} == 'TEST_USER_ID'"""
        )
    )
    filter_intersection = PydanticWhereFilterIntersection(where_filters=[time_filter, entity_filter])

    parse_result = dict(filter_intersection.filter_expression_parameter_sets(custom_granularity_names=()))

    assert parse_result.get(time_filter.where_sql_template) == JinjaCallParameterSets(
        time_dimension_call_parameter_sets=(
            TimeDimensionCallParameterSet(
                time_dimension_reference=TimeDimensionReference(element_name="metric_time"),
                entity_path=(),
                time_granularity_name=TimeGranularity.MONTH.value,
            ),
        )
    )
    assert parse_result.get(entity_filter.where_sql_template) == JinjaCallParameterSets(
        dimension_call_parameter_sets=(),
        entity_call_parameter_sets=(
            EntityCallParameterSet(
                entity_path=(),
                entity_reference=EntityReference("listing"),
            ),
            EntityCallParameterSet(
                entity_path=(EntityReference("listing"),),
                entity_reference=EntityReference("user"),
            ),
        ),
    )


def test_where_filter_intersection_error_collection() -> None:
    """Tests the error behaviors when parsing where filters and collecting the call parameter sets for each.

    This should result in a single exception with all broken filters represented.
    """
    metric_time_in_dimension_error = PydanticWhereFilter(
        where_sql_template="{{ TimeDimension('order_id__order_time__month', 'week') }} > '2020-01-01'"
    )
    valid_dimension = PydanticWhereFilter(where_sql_template=" {Dimension('customer__has_delivery_address')} ")
    entity_format_error = PydanticWhereFilter(where_sql_template="{{ Entity('order_id__is_food_order__day') }}")
    filter_intersection = PydanticWhereFilterIntersection(
        where_filters=[metric_time_in_dimension_error, valid_dimension, entity_format_error]
    )

    with pytest.raises(ParseJinjaObjectException) as exc_info:
        filter_intersection.filter_expression_parameter_sets(custom_granularity_names=())

    error_string = str(exc_info.value)
    # These are a little too implementation-specific, but it demonstrates that we are collecting the errors we find.
    assert "Received different grains in `time_dimension_name` parameter" in error_string
    assert "It should not contain a time grain suffix." in error_string


def test_time_dimension_without_granularity() -> None:  # noqa: D103
    parse_result = PydanticWhereFilter(
        where_sql_template="{{ TimeDimension('booking__created_at') }} > 2023-09-18"
    ).call_parameter_sets(custom_granularity_names=())

    assert parse_result == JinjaCallParameterSets(
        dimension_call_parameter_sets=(),
        time_dimension_call_parameter_sets=(
            TimeDimensionCallParameterSet(
                entity_path=(EntityReference("booking"),),
                time_dimension_reference=TimeDimensionReference(element_name="created_at"),
                time_granularity_name=None,
            ),
        ),
        entity_call_parameter_sets=(),
    )


def test_time_dimension_with_custom_granularity() -> None:  # noqa: D103
    parse_result = PydanticWhereFilter(
        where_sql_template="{{ TimeDimension('booking__created_at', 'martian_week') }} > 2023-09-18"
    ).call_parameter_sets(custom_granularity_names=("martian_week",))

    assert parse_result == JinjaCallParameterSets(
        dimension_call_parameter_sets=(),
        time_dimension_call_parameter_sets=(
            TimeDimensionCallParameterSet(
                entity_path=(EntityReference("booking"),),
                time_dimension_reference=TimeDimensionReference(element_name="created_at"),
                time_granularity_name="martian_week",
            ),
        ),
        entity_call_parameter_sets=(),
    )
