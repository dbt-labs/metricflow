from __future__ import annotations

import logging
import textwrap
from typing import List

import pytest
from dbt_semantic_interfaces.parsing.dir_to_model import parse_yaml_files_to_validation_ready_semantic_manifest
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.test_utils import as_datetime
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from dbt_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator

from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
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
from metricflow.test.model.example_project_configuration import EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE
from metricflow.test.time.metric_time_dimension import MTD

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
        - name: revenue_daily
          expr: revenue
          agg: sum
          create_metric: true
          agg_time_dimension: loaded_at


      dimensions:
        - name: ds
          type: time
          expr: created_at
          type_params:
            time_granularity: month
        - name: loaded_at
          type: time
          expr: created_at
          type_params:
            time_granularity: day
        - name: country
          type: categorical
          expr: country

      primary_entity: revenue_instance

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
    ---
    metric:
      name: monthly_revenue_last_7_days
      description: Derived offset metric with 2 different agg_time_dimensions
      type: derived
      type_params:
        expr: revenue - revenue_daily
        metrics:
          - name: revenue
          - name: revenue_daily
            offset_window: 1 week
            alias: revenue_last_7_days
    """
)


@pytest.fixture
def bookings_query_parser() -> MetricFlowQueryParser:  # noqa
    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=BOOKINGS_YAML)
    return query_parser_from_yaml([EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, bookings_yaml_file])


@pytest.fixture
def revenue_query_parser() -> MetricFlowQueryParser:  # noqa
    revenue_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=REVENUE_YAML)
    return query_parser_from_yaml([EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, revenue_yaml_file])


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


def test_query_parser_case_insensitivity(bookings_query_parser: MetricFlowQueryParser) -> None:  # noqa: D
    # String params
    query_spec = bookings_query_parser.parse_and_validate_query(
        metric_names=["BOOKINGS"],
        group_by_names=["BOOKING__IS_INSTANT", "LISTING", MTD.upper()],
        order_by_names=[MTD.upper(), "-BOOKINGS"],
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

    # Object params
    metric = MetricParameter(name="BOOKINGS")
    group_by = (
        DimensionOrEntityParameter("BOOKING__IS_INSTANT"),
        DimensionOrEntityParameter("LISTING"),
        TimeDimensionParameter(MTD.upper()),
    )
    order_by = (
        OrderByParameter(order_by=TimeDimensionParameter(MTD.upper())),
        OrderByParameter(order_by=MetricParameter("BOOKINGS"), descending=True),
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


def test_query_parser_invalid_group_by(bookings_query_parser: MetricFlowQueryParser) -> None:  # noqa: D
    with pytest.raises(InvalidQueryException):
        bookings_query_parser.parse_and_validate_query(group_by_names=["random_stuff"])


def test_query_parser_with_object_params(bookings_query_parser: MetricFlowQueryParser) -> None:  # noqa: D
    metric = MetricParameter(name="bookings")
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
    # check constraint on invalid_dim raises InvalidQueryException
    with pytest.raises(InvalidQueryException, match="does not match any of the available"):
        bookings_query_parser.parse_and_validate_query(
            metric_names=["bookings"],
            group_by_names=[MTD],
            time_constraint_start=as_datetime("2020-01-15"),
            time_constraint_end=as_datetime("2020-02-15"),
            where_constraint_str="{{ Dimension('booking__invalid_dim') }} = '1'",
        )

    with pytest.raises(InvalidQueryException, match="Error parsing where filter"):
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
    with pytest.raises(InvalidQueryException, match="does not match any of the available"):
        query_parser.parse_and_validate_query(
            metric_names=["revenue"],
            group_by_names=[MTD],
            where_constraint_str="{{ TimeDimension('metric_time', 'day') }} > '2020-01-15'",
        )


def test_parse_and_validate_metric_constraint_dims() -> None:
    """Test that the returned time constraint in the query spec is adjusted to match the granularity of the query.

    TODO: This test doesn't do what it says it does.
    """
    revenue_yaml_file = YamlConfigFile(filepath="inline_for_test_2", contents=REVENUE_YAML)
    query_parser = query_parser_from_yaml([EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, revenue_yaml_file])

    # check constraint on invalid_dim raises InvalidQueryException
    with pytest.raises(InvalidQueryException, match="given input does not exactly match"):
        query_parser.parse_and_validate_query(
            metric_names=["metric_with_invalid_constraint"],
            group_by_names=[MTD],
            time_constraint_start=as_datetime("2020-01-15"),
            time_constraint_end=as_datetime("2020-02-15"),
        )


def test_cumulative_metric_no_time_dimension_validation() -> None:
    """Test that queries for cumulative metrics fail if no time dimensions are selected.

    This is a test of validation enforcement to ensure users don't get incorrect results due to current
    limitations, and should be deleted or updated when this restriction is lifted.
    """
    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=BOOKINGS_YAML)
    metrics_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=METRICS_YAML)
    revenue_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=REVENUE_YAML)
    query_parser = query_parser_from_yaml(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, bookings_yaml_file, revenue_yaml_file, metrics_yaml_file]
    )

    with pytest.raises(InvalidQueryException, match="do not include 'metric_time'"):
        query_parser.parse_and_validate_query(
            metric_names=["revenue_cumulative"],
        )


def test_cumulative_metric_wrong_time_dimension_validation() -> None:
    """Test that queries for cumulative metrics fail if the agg_time_dimension is not selected.

    Our current behavior for cases where a different time dimension is selected by the agg_time_dimension is
    not is undefined. Until we add support for grouping by a different time dimension for a cumulative metric
    computed against metric_time, overriding the agg_time_dimension at query time, or both, this query is
    restricted.

    This is a test of validation enforcement to ensure users don't get incorrect results due to current
    limitations, and should be deleted or updated when this restriction is lifted.
    """
    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=BOOKINGS_YAML)
    metrics_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=METRICS_YAML)
    revenue_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=REVENUE_YAML)
    query_parser = query_parser_from_yaml(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, bookings_yaml_file, revenue_yaml_file, metrics_yaml_file]
    )

    with pytest.raises(InvalidQueryException, match="do not include 'metric_time'"):
        query_parser.parse_and_validate_query(
            metric_names=["revenue_cumulative"],
            group_by_names=["revenue_instance__loaded_at"],
        )


def test_cumulative_metric_agg_time_dimension_name_validation() -> None:
    """Test that queries for cumulative metrics succeed if the agg_time_dimension is selected by name."""
    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=BOOKINGS_YAML)
    metrics_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=METRICS_YAML)
    revenue_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=REVENUE_YAML)
    query_parser = query_parser_from_yaml(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, bookings_yaml_file, revenue_yaml_file, metrics_yaml_file]
    )

    query_parser.parse_and_validate_query(metric_names=["revenue_cumulative"], group_by_names=["revenue_instance__ds"])


def test_derived_metric_query_parsing() -> None:
    """Test derived metric inputs are properly validated."""
    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=BOOKINGS_YAML)
    metrics_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=METRICS_YAML)
    revenue_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=REVENUE_YAML)
    query_parser = query_parser_from_yaml(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, bookings_yaml_file, revenue_yaml_file, metrics_yaml_file]
    )
    # Attempt to query with no dimension
    with pytest.raises(InvalidQueryException, match="do not include 'metric_time'"):
        query_parser.parse_and_validate_query(
            metric_names=["revenue_sub_10"],
            group_by_names=[],
        )

    # Attempt to query with non-time dimension
    with pytest.raises(InvalidQueryException, match="does not match any of the available"):
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
    with pytest.raises(InvalidQueryException, match="do not include 'metric_time'"):
        query_parser.parse_and_validate_query(
            metric_names=["revenue_growth_2_weeks"],
            group_by_names=[],
        )

    # Attempt to query with non-time dimension
    with pytest.raises(InvalidQueryException, match="do not include 'metric_time'"):
        query_parser.parse_and_validate_query(
            metric_names=["revenue_growth_2_weeks"],
            group_by_names=["revenue_instance__country"],
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
    with pytest.raises(InvalidQueryException, match="does not match any of the available"):
        query_parser.parse_and_validate_query(
            metric_names=["revenue"],
            group_by=(TimeDimensionParameter(name="metric_time", date_part=DatePart.DOW),),
        )

    # Can't query date part for cumulative metrics
    with pytest.raises(InvalidQueryException, match="does not match any of the available"):
        query_parser.parse_and_validate_query(
            metric_names=["revenue_cumulative"],
            group_by=(TimeDimensionParameter(name="metric_time", date_part=DatePart.YEAR),),
        )

    # Can't query date part for metrics with offset to grain
    with pytest.raises(InvalidQueryException, match="does not allow group-by-items with a date part in the query"):
        query_parser.parse_and_validate_query(
            metric_names=["revenue_since_start_of_year"],
            group_by=(TimeDimensionParameter(name="metric_time", date_part=DatePart.MONTH),),
        )

    # Requested granularity doesn't match resolved granularity
    with pytest.raises(InvalidQueryException, match="does not match any of the available"):
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


def test_duplicate_metric_query(bookings_query_parser: MetricFlowQueryParser) -> None:  # noqa: D
    with pytest.raises(InvalidQueryException, match="duplicate metrics"):
        bookings_query_parser.parse_and_validate_query(
            metric_names=["bookings", "bookings"],
            group_by_names=[MTD],
        )


def test_no_metrics_or_group_by(bookings_query_parser: MetricFlowQueryParser) -> None:  # noqa: D
    with pytest.raises(InvalidQueryException, match="no metrics or group by items"):
        bookings_query_parser.parse_and_validate_query()


def test_offset_metric_with_diff_agg_time_dims_error() -> None:  # noqa: D
    metrics_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=METRICS_YAML)
    revenue_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=REVENUE_YAML)
    query_parser = query_parser_from_yaml(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, revenue_yaml_file, metrics_yaml_file]
    )
    with pytest.raises(InvalidQueryException, match="does not match any of the available group-by-items"):
        query_parser.parse_and_validate_query(
            metric_names=["monthly_revenue_last_7_days"],
            group_by_names=["revenue___ds"],
        )


def query_parser_from_yaml(yaml_contents: List[YamlConfigFile]) -> MetricFlowQueryParser:
    """Given yaml files, return a query parser using default source nodes, resolvers and time spine source."""
    semantic_manifest_lookup = SemanticManifestLookup(
        parse_yaml_files_to_validation_ready_semantic_manifest(
            yaml_contents, apply_transformations=True
        ).semantic_manifest
    )
    SemanticManifestValidator[SemanticManifest]().checked_validations(semantic_manifest_lookup.semantic_manifest)
    return MetricFlowQueryParser(
        semantic_manifest_lookup=semantic_manifest_lookup,
    )
