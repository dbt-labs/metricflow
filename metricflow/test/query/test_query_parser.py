from __future__ import annotations

import logging
import textwrap
from collections import namedtuple

import pytest
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.test_utils import as_datetime
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.errors.errors import UnableToSatisfyQueryError
from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.query.query_exceptions import InvalidQueryException
from metricflow.query.query_parser import MetricFlowQueryParser
from metricflow.specs.query_param_implementations import (
    DimensionOrEntityParameter,
    MetricParameter,
    OrderByParameter,
    TimeDimensionParameter,
)
from metricflow.specs.specs import (
    DimensionSpec,
    EntitySpec,
    MetricSpec,
    OrderBySpec,
    TimeDimensionSpec,
)
from metricflow.test.fixtures.model_fixtures import query_parser_from_yaml
from metricflow.test.model.example_project_configuration import EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE
from metricflow.test.time.metric_time_dimension import MTD
from metricflow.time.date_part import DatePart
from metricflow.time.time_granularity_solver import RequestTimeGranularityException

logger = logging.getLogger(__name__)


BOOKINGS_YAML = textwrap.dedent(
    """\
    semantic_model:
      name: bookings_source

      node_relation:
        schema_name: some_schema
        alias: bookings_source_table

      defaults:
        agg_time_dimension: ds

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
            time_granularity: day

      primary_entity: booking

      entities:
        - name: listing
          type: foreign
          expr: listing_id
    """
)


REVENUE_YAML = textwrap.dedent(
    """\
    semantic_model:
      name: revenue_source
      description: revenue

      node_relation:
        schema_name: some_schema
        alias: fct_revenue_table

      defaults:
        agg_time_dimension: ds

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
            time_granularity: month
        - name: country
          type: categorical
          expr: country

      primary_entity: company

      entities:
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
      type: cumulative
      type_params:
        measure: revenue
        window: 7 days
    ---
    metric:
      name: revenue_sub_10
      description: Derived cumulative metric for revenue for testing purposes
      type: derived
      type_params:
        expr: revenue_cumulative - 10
        metrics:
          - name: revenue_cumulative
    ---
    metric:
      name: revenue_growth_2_weeks
      description: Percentage growth of revenue compared to revenue 2 weeks prior
      type: derived
      type_params:
        expr: (revenue - revenue_2_weeks_ago) / revenue_2_weeks_ago
        metrics:
          - name: revenue
          - name: revenue
            offset_window: 14 days
            alias: revenue_2_weeks_ago
    ---
    metric:
      name: revenue_since_start_of_year
      description: Revenue since start of year
      type: derived
      type_params:
        expr: revenue - revenue_start_of_year
        metrics:
          - name: revenue
          - name: revenue
            offset_to_grain: year
            alias: revenue_start_of_year
    """
)


@pytest.fixture
def bookings_query_parser() -> MetricFlowQueryParser:  # noqa
    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=BOOKINGS_YAML)
    return query_parser_from_yaml([EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, bookings_yaml_file])


def test_query_parser(bookings_query_parser: MetricFlowQueryParser) -> None:  # noqa: D
    query_spec = bookings_query_parser.parse_and_validate_query(
        metric_names=["bookings"],
        group_by_names=["booking__is_instant", "listing", MTD],
        order_by_names=[MTD, "-bookings"],
    )

    assert query_spec.metric_specs == (MetricSpec(element_name="bookings"),)
    assert query_spec.dimension_specs == (
        DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),
    )
    assert query_spec.time_dimension_specs == (
        TimeDimensionSpec(element_name=MTD, entity_links=(), time_granularity=TimeGranularity.DAY),
    )
    assert query_spec.entity_specs == (EntitySpec(element_name="listing", entity_links=()),)
    assert query_spec.order_by_specs == (
        OrderBySpec(
            instance_spec=TimeDimensionSpec(element_name=MTD, entity_links=(), time_granularity=TimeGranularity.DAY),
            descending=False,
        ),
        OrderBySpec(
            instance_spec=MetricSpec(element_name="bookings"),
            descending=True,
        ),
    )


def test_query_parser_with_object_params(bookings_query_parser: MetricFlowQueryParser) -> None:  # noqa: D
    Metric = namedtuple("Metric", ["name", "descending"])
    metric = Metric("bookings", False)
    group_by = (
        DimensionOrEntityParameter("booking__is_instant"),
        DimensionOrEntityParameter("listing"),
        TimeDimensionParameter(MTD),
    )
    order_by = (
        OrderByParameter(order_by=TimeDimensionParameter(MTD)),
        OrderByParameter(order_by=MetricParameter("bookings"), descending=True),
    )
    query_spec = bookings_query_parser.parse_and_validate_query(metrics=[metric], group_by=group_by, order_by=order_by)
    assert query_spec.metric_specs == (MetricSpec(element_name="bookings"),)
    assert query_spec.dimension_specs == (
        DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),
    )
    assert query_spec.time_dimension_specs == (
        TimeDimensionSpec(element_name=MTD, entity_links=(), time_granularity=TimeGranularity.DAY),
    )
    assert query_spec.entity_specs == (EntitySpec(element_name="listing", entity_links=()),)
    assert query_spec.order_by_specs == (
        OrderBySpec(
            instance_spec=TimeDimensionSpec(element_name=MTD, entity_links=(), time_granularity=TimeGranularity.DAY),
            descending=False,
        ),
        OrderBySpec(
            instance_spec=MetricSpec(element_name="bookings"),
            descending=True,
        ),
    )


def test_order_by_granularity_conversion() -> None:
    """Test that the granularity of the primary time dimension in the order by is returned appropriately.

    In the case where the primary time dimension is specified in the order by without a granularity suffix, the order
    by spec returned by the parser should have a granularity appropriate for the queried metrics.
    """
    # "bookings" has a granularity of DAY, "revenue" has a granularity of MONTH
    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=BOOKINGS_YAML)
    revenue_yaml_file = YamlConfigFile(filepath="inline_for_test_2", contents=REVENUE_YAML)

    query_parser = query_parser_from_yaml(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, bookings_yaml_file, revenue_yaml_file]
    )
    query_spec = query_parser.parse_and_validate_query(
        metric_names=["bookings", "revenue"], group_by_names=[MTD], order_by_names=[f"-{MTD}"]
    )

    # The lowest common granularity is MONTH, so we expect the PTD in the order by to have that granularity.
    assert (
        OrderBySpec(
            instance_spec=TimeDimensionSpec(element_name=MTD, entity_links=(), time_granularity=TimeGranularity.MONTH),
            descending=True,
        ),
    ) == query_spec.order_by_specs


def test_order_by_granularity_no_conversion(bookings_query_parser: MetricFlowQueryParser) -> None:  # noqa: D
    query_spec = bookings_query_parser.parse_and_validate_query(
        metric_names=["bookings"], group_by_names=[MTD], order_by_names=[MTD]
    )

    # The only granularity is DAY, so we expect the PTD in the order by to have that granularity.
    assert (
        OrderBySpec(
            instance_spec=TimeDimensionSpec(element_name=MTD, entity_links=(), time_granularity=TimeGranularity.DAY),
            descending=False,
        ),
    ) == query_spec.order_by_specs


def test_time_range_constraint_conversion() -> None:
    """Test that the returned time constraint in the query spec is adjusted to match the granularity of the query."""
    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=BOOKINGS_YAML)
    revenue_yaml_file = YamlConfigFile(filepath="inline_for_test_2", contents=REVENUE_YAML)

    query_parser = query_parser_from_yaml(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, bookings_yaml_file, revenue_yaml_file]
    )

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


def test_parse_and_validate_where_constraint_dims(bookings_query_parser: MetricFlowQueryParser) -> None:
    """Test that the returned time constraint in the query spec is adjusted to match the granularity of the query."""
    # check constraint on invalid_dim raises UnableToSatisfyQueryError
    with pytest.raises(UnableToSatisfyQueryError):
        bookings_query_parser.parse_and_validate_query(
            metric_names=["bookings"],
            group_by_names=[MTD],
            time_constraint_start=as_datetime("2020-01-15"),
            time_constraint_end=as_datetime("2020-02-15"),
            where_constraint_str="{{ Dimension('booking__invalid_dim') }} = '1'",
        )

    with pytest.raises(InvalidQueryException):
        bookings_query_parser.parse_and_validate_query(
            metric_names=["bookings"],
            group_by_names=[MTD],
            time_constraint_start=as_datetime("2020-01-15"),
            time_constraint_end=as_datetime("2020-02-15"),
            where_constraint_str="{{ Dimension('invalid_format') }} = '1'",
        )

    query_spec = bookings_query_parser.parse_and_validate_query(
        metric_names=["bookings"],
        group_by_names=[MTD],
        time_constraint_start=as_datetime("2020-01-15"),
        time_constraint_end=as_datetime("2020-02-15"),
        where_constraint_str="{{ Dimension('booking__is_instant') }} = '1'",
    )
    assert (
        DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),))
        not in query_spec.dimension_specs
    )


def test_parse_and_validate_where_constraint_metric_time() -> None:
    """Test that granularity of metric_time reference in where constraint is at least that of the ds dimension."""
    revenue_yaml_file = YamlConfigFile(filepath="inline_for_test_2", contents=REVENUE_YAML)

    query_parser = query_parser_from_yaml([EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, revenue_yaml_file])
    with pytest.raises(RequestTimeGranularityException):
        query_parser.parse_and_validate_query(
            metric_names=["revenue"],
            group_by_names=[MTD],
            where_constraint_str="{{ TimeDimension('metric_time', 'day') }} > '2020-01-15'",
        )


def test_parse_and_validate_metric_constraint_dims() -> None:
    """Test that the returned time constraint in the query spec is adjusted to match the granularity of the query."""
    revenue_yaml_file = YamlConfigFile(filepath="inline_for_test_2", contents=REVENUE_YAML)
    query_parser = query_parser_from_yaml([EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, revenue_yaml_file])

    # check constraint on invalid_dim raises UnableToSatisfyQueryError
    with pytest.raises(UnableToSatisfyQueryError):
        query_parser.parse_and_validate_query(
            metric_names=["metric_with_invalid_constraint"],
            group_by_names=[MTD],
            time_constraint_start=as_datetime("2020-01-15"),
            time_constraint_end=as_datetime("2020-02-15"),
        )


def test_derived_metric_query_parsing() -> None:
    """Test derived metric inputs are properly validated."""
    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=BOOKINGS_YAML)
    metrics_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=METRICS_YAML)
    revenue_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=REVENUE_YAML)
    query_parser = query_parser_from_yaml(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, bookings_yaml_file, revenue_yaml_file, metrics_yaml_file]
    )
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


def test_derived_metric_with_offset_parsing() -> None:
    """Test that querying derived metrics with a time offset requires a time dimension."""
    revenue_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=REVENUE_YAML)
    metrics_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=METRICS_YAML)
    query_parser = query_parser_from_yaml(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, revenue_yaml_file, metrics_yaml_file]
    )
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


def test_date_part_parsing() -> None:
    """Test that querying with a date_part verifies compatibility with time_granularity."""
    revenue_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=REVENUE_YAML)
    metrics_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=METRICS_YAML)
    query_parser = query_parser_from_yaml(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, revenue_yaml_file, metrics_yaml_file]
    )

    # Date part is incompatible with metric's defined time granularity
    with pytest.raises(RequestTimeGranularityException):
        query_parser.parse_and_validate_query(
            metric_names=["revenue"],
            group_by=(TimeDimensionParameter(name="metric_time", date_part=DatePart.DOW),),
        )

    # Can't query date part for cumulative metrics
    with pytest.raises(UnableToSatisfyQueryError):
        query_parser.parse_and_validate_query(
            metric_names=["revenue_cumulative"],
            group_by=(TimeDimensionParameter(name="metric_time", date_part=DatePart.YEAR),),
        )

    # Can't query date part for metrics with offset to grain
    with pytest.raises(UnableToSatisfyQueryError):
        query_parser.parse_and_validate_query(
            metric_names=["revenue_since_start_of_year"],
            group_by=(TimeDimensionParameter(name="metric_time", date_part=DatePart.MONTH),),
        )

    # Requested granularity doesn't match resolved granularity
    with pytest.raises(RequestTimeGranularityException):
        query_parser.parse_and_validate_query(
            metric_names=["revenue"],
            group_by=(
                TimeDimensionParameter(name="metric_time", grain=TimeGranularity.YEAR, date_part=DatePart.MONTH),
            ),
        )

    # Date part is compatible
    query_parser.parse_and_validate_query(
        metric_names=["revenue"],
        group_by=(TimeDimensionParameter(name="metric_time", date_part=DatePart.MONTH),),
    )
