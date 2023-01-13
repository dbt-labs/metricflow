import logging
import textwrap

import pytest

from metricflow.constraints.time_constraint import TimeRangeConstraint
from metricflow.errors.errors import UnableToSatisfyQueryError
from metricflow.model.objects.common import YamlConfigFile
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.specs import (
    MetricSpec,
    DimensionSpec,
    TimeDimensionSpec,
    IdentifierSpec,
    OrderBySpec,
    OutputColumnNameOverride,
)
from metricflow.test.test_utils import as_datetime
from metricflow.test.time.metric_time_dimension import MTD
from metricflow.time.time_granularity import TimeGranularity
from metricflow.time.time_granularity_solver import RequestTimeGranularityException
from metricflow.test.fixtures.model_fixtures import query_parser_from_yaml

logger = logging.getLogger(__name__)


BOOKINGS_YAML = textwrap.dedent(
    """\
    data_source:
      name: bookings_source

      sql_query: |
        -- User Defined SQL Query
        SELECT * FROM bookings_source_table

      measures:
        - name: bookings
          expr: "1"
          agg: sum
          create_metric: true

      dimensions:
        - name: is_instant
          type: categorical
        - name: ds
          type: time
          type_params:
            is_primary: True
            time_granularity: day

      identifiers:
        - name: listing
          type: foreign
          expr: listing_id
    """
)


REVENUE_YAML = textwrap.dedent(
    """\
    data_source:
      name: revenue_source
      description: revenue
      owners:
        - support@transformdata.io

      sql_query: |
        -- User Defined SQL Query
        SELECT * FROM fct_revenue_table

      measures:
        - name: revenue
          expr: revenue
          agg: sum
          create_metric: true

      dimensions:
        - name: ds
          type: time
          expr: created_at
          type_params:
            is_primary: True
            time_granularity: month
        - name: country
          type: categorical
          expr: country

      identifiers:
        - name: user
          type: foreign
          expr: user_id
    """
)

METRICS_YAML = textwrap.dedent(
    """\
    ---
    metric:
      name: revenue_cumulative
      description: Cumulative metric for revenue for testing purposes
      owners:
        - support@transformdata.io
      type: cumulative
      type_params:
        measures:
          - revenue
        window: 7 days
    ---
    metric:
      name: revenue_sub_10
      description: Derived cumulative metric for revenue for testing purposes
      owners:
        - support@transformdata.io
      type: derived
      type_params:
        expr: revenue_cumulative - 10
        metrics:
          - name: revenue_cumulative
    ---
    metric:
      name: revenue_growth_2_weeks
      description: Percentage growth of revenue compared to revenue 2 weeks prior
      owners:
        - support@transformdata.io
      type: derived
      type_params:
        expr: (revenue - revenue_2_weeks_ago) / revenue_2_weeks_ago
        metrics:
          - name: revenue
          - name: revenue
            offset_window: 14 days
            alias: revenue_2_weeks_ago
    """
)


def test_query_parser(time_spine_source: TimeSpineSource) -> None:  # noqa: D
    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=BOOKINGS_YAML)
    query_parser = query_parser_from_yaml([bookings_yaml_file], time_spine_source)

    query_spec = query_parser.parse_and_validate_query(
        metric_names=["bookings"], group_by_names=["is_instant", "listing", MTD], order=[MTD, "-bookings"]
    )

    assert query_spec.metric_specs == (MetricSpec(element_name="bookings"),)
    assert query_spec.dimension_specs == (DimensionSpec(element_name="is_instant", identifier_links=()),)
    assert query_spec.time_dimension_specs == (
        TimeDimensionSpec(element_name=MTD, identifier_links=(), time_granularity=TimeGranularity.DAY),
    )
    assert query_spec.identifier_specs == (IdentifierSpec(element_name="listing", identifier_links=()),)
    assert query_spec.order_by_specs == (
        OrderBySpec(
            time_dimension_spec=TimeDimensionSpec(
                element_name=MTD, identifier_links=(), time_granularity=TimeGranularity.DAY
            ),
            descending=False,
        ),
        OrderBySpec(
            metric_spec=MetricSpec(element_name="bookings"),
            descending=True,
        ),
    )


def test_order_by_granularity_conversion(time_spine_source: TimeSpineSource) -> None:
    """Test that the granularity of the primary time dimension in the order by is returned appropriately.

    In the case where the primary time dimension is specified in the order by without a granularity suffix, the order
    by spec returned by the parser should have a granularity appropriate for the queried metrics.
    """

    # "bookings" has a granularity of DAY, "revenue" has a granularity of MONTH
    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=BOOKINGS_YAML)
    revenue_yaml_file = YamlConfigFile(filepath="inline_for_test_2", contents=REVENUE_YAML)

    query_parser = query_parser_from_yaml([bookings_yaml_file, revenue_yaml_file], time_spine_source)
    query_spec = query_parser.parse_and_validate_query(
        metric_names=["bookings", "revenue"], group_by_names=[MTD], order=[f"-{MTD}"]
    )

    # The lowest common granularity is MONTH, so we expect the PTD in the order by to have that granularity.
    assert (
        OrderBySpec(
            time_dimension_spec=TimeDimensionSpec(
                element_name=MTD, identifier_links=(), time_granularity=TimeGranularity.MONTH
            ),
            descending=True,
        ),
    ) == query_spec.order_by_specs


def test_order_by_granularity_no_conversion(time_spine_source: TimeSpineSource) -> None:  # noqa: D

    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=BOOKINGS_YAML)

    query_parser = query_parser_from_yaml([bookings_yaml_file], time_spine_source)

    query_spec = query_parser.parse_and_validate_query(metric_names=["bookings"], group_by_names=[MTD], order=[MTD])

    # The only granularity is DAY, so we expect the PTD in the order by to have that granularity.
    assert (
        OrderBySpec(
            time_dimension_spec=TimeDimensionSpec(
                element_name=MTD, identifier_links=(), time_granularity=TimeGranularity.DAY
            ),
            descending=False,
        ),
    ) == query_spec.order_by_specs


def test_time_range_constraint_conversion(time_spine_source: TimeSpineSource) -> None:
    """Test that the returned time constraint in the query spec is adjusted to match the granularity of the query."""
    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=BOOKINGS_YAML)
    revenue_yaml_file = YamlConfigFile(filepath="inline_for_test_2", contents=REVENUE_YAML)

    query_parser = query_parser_from_yaml([bookings_yaml_file, revenue_yaml_file], time_spine_source)

    # "bookings" has a granularity of DAY, "revenue" has a granularity of MONTH
    query_spec = query_parser.parse_and_validate_query(
        metric_names=["bookings", "revenue"],
        group_by_names=[MTD],
        time_constraint_start=as_datetime("2020-01-15"),
        time_constraint_end=as_datetime("2020-02-15"),
    )

    assert (
        TimeRangeConstraint(start_time=as_datetime("2020-01-01"), end_time=as_datetime("2020-02-29"))
    ) == query_spec.time_range_constraint


def test_column_override(time_spine_source: TimeSpineSource) -> None:
    """Tests that the output column override is set.

    Should be set in cases where the metrics have a non-day granularity, but ds is specified.
    """

    revenue_yaml_file = YamlConfigFile(filepath="inline_for_test_2", contents=REVENUE_YAML)

    query_parser = query_parser_from_yaml([revenue_yaml_file], time_spine_source)

    # "revenue" has a granularity of MONTH
    query_spec = query_parser.parse_and_validate_query(
        metric_names=["revenue"],
        group_by_names=[MTD],
        time_constraint_start=as_datetime("2020-01-15"),
        time_constraint_end=as_datetime("2020-02-15"),
    )

    assert (
        OutputColumnNameOverride(
            time_dimension_spec=TimeDimensionSpec(
                element_name=MTD,
                identifier_links=(),
                time_granularity=TimeGranularity.MONTH,
            ),
            output_column_name=MTD,
        ),
    ) == query_spec.output_column_name_overrides


def test_parse_and_validate_where_constraint_dims(time_spine_source: TimeSpineSource) -> None:
    """Test that the returned time constraint in the query spec is adjusted to match the granularity of the query."""
    # check constraint on invalid_dim raises UnableToSatisfyQueryError

    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=BOOKINGS_YAML)

    query_parser = query_parser_from_yaml([bookings_yaml_file], time_spine_source)

    with pytest.raises(UnableToSatisfyQueryError):
        query_parser.parse_and_validate_query(
            metric_names=["bookings"],
            group_by_names=[MTD],
            time_constraint_start=as_datetime("2020-01-15"),
            time_constraint_end=as_datetime("2020-02-15"),
            where_constraint_str="WHERE invalid_dim = '1'",
        )

    query_spec = query_parser.parse_and_validate_query(
        metric_names=["bookings"],
        group_by_names=[MTD],
        time_constraint_start=as_datetime("2020-01-15"),
        time_constraint_end=as_datetime("2020-02-15"),
        where_constraint_str="WHERE is_instant = '1'",
    )
    assert DimensionSpec(element_name="is_instant", identifier_links=()) not in query_spec.dimension_specs


def test_parse_and_validate_where_constraint_metric_time(time_spine_source: TimeSpineSource) -> None:
    """Test that granularity of metric_time reference in where constraint is at least that of the ds dimension."""
    revenue_yaml_file = YamlConfigFile(filepath="inline_for_test_2", contents=REVENUE_YAML)

    query_parser = query_parser_from_yaml([revenue_yaml_file], time_spine_source)
    with pytest.raises(RequestTimeGranularityException):
        query_parser.parse_and_validate_query(
            metric_names=["revenue"],
            group_by_names=[MTD],
            where_constraint_str="WHERE metric_time__day > '2020-01-15'",
        )


def test_parse_and_validate_metric_constraint_dims(time_spine_source: TimeSpineSource) -> None:
    """Test that the returned time constraint in the query spec is adjusted to match the granularity of the query."""

    revenue_yaml_file = YamlConfigFile(filepath="inline_for_test_2", contents=REVENUE_YAML)
    query_parser = query_parser_from_yaml([revenue_yaml_file], time_spine_source)

    # check constraint on invalid_dim raises UnableToSatisfyQueryError
    with pytest.raises(UnableToSatisfyQueryError):
        query_parser.parse_and_validate_query(
            metric_names=["metric_with_invalid_constraint"],
            group_by_names=[MTD],
            time_constraint_start=as_datetime("2020-01-15"),
            time_constraint_end=as_datetime("2020-02-15"),
        )


def test_derived_metric_query_parsing(time_spine_source: TimeSpineSource) -> None:
    """Test derived metric inputs are properly validated."""

    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=BOOKINGS_YAML)
    metrics_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=METRICS_YAML)
    revenue_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=REVENUE_YAML)
    query_parser = query_parser_from_yaml([bookings_yaml_file, revenue_yaml_file, metrics_yaml_file], time_spine_source)
    # Attempt to query with no dimension
    with pytest.raises(UnableToSatisfyQueryError):
        query_parser.parse_and_validate_query(
            metric_names=["revenue_sub_10"],
            group_by_names=[],
        )

    # Attempt to query with non-time dimension
    with pytest.raises(UnableToSatisfyQueryError):
        query_parser.parse_and_validate_query(
            metric_names=["revenue_sub_10"],
            group_by_names=["country"],
        )

    # Query with time dimension
    query_parser.parse_and_validate_query(
        metric_names=["revenue_sub_10"],
        group_by_names=[MTD],
    )


def test_derived_metric_with_offset_parsing(time_spine_source: TimeSpineSource) -> None:
    """Test that querying derived metrics with a time offset requires a time dimension."""
    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=BOOKINGS_YAML)
    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=REVENUE_YAML)
    metrics_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=METRICS_YAML)
    query_parser = query_parser_from_yaml([bookings_yaml_file, metrics_yaml_file], time_spine_source)
    # Attempt to query with no dimension
    with pytest.raises(UnableToSatisfyQueryError):
        query_parser.parse_and_validate_query(
            metric_names=["revenue_growth_2_weeks"],
            group_by_names=[],
        )

    # Attempt to query with non-time dimension
    with pytest.raises(UnableToSatisfyQueryError):
        query_parser.parse_and_validate_query(
            metric_names=["revenue_growth_2_weeks"],
            group_by_names=["country"],
        )

    # Query with time dimension
    query_parser.parse_and_validate_query(
        metric_names=["revenue_growth_2_weeks"],
        group_by_names=[MTD],
    )
