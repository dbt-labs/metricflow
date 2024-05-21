"""Tests for rendering queries requiring joins against a time spine dataset.

This module is specifically for query rendering, rather than time spine join node behavior. It is
meant to encompass both the basis of the null coalesce time spine join rendering configuration
and other time-spine related features, such as exploding a time range aggregation into a set of
rows covering each sub-interval in the range (e.g., for measure defined in SCD datasets)
"""

from __future__ import annotations

import datetime

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.specs.spec_classes import MetricSpec
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataset.dataset_classes import DataSet
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.query_rendering.compare_rendered_query import convert_and_check


@pytest.mark.sql_engine_snapshot
def test_simple_join_to_time_spine(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_join_to_time_spine"),),
            time_dimension_specs=(DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY),),
        )
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )


@pytest.mark.sql_engine_snapshot
def test_simple_join_to_time_spine_with_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Test case where metric fills nulls and filter is not in group by. Should apply constraint once."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_fill_nulls_with_0",),
        group_by_names=("metric_time__day",),
        where_constraint=PydanticWhereFilter(where_sql_template="{{ Dimension('booking__is_instant') }}"),
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )


@pytest.mark.sql_engine_snapshot
def test_simple_join_to_time_spine_with_queried_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Test case where metric fills nulls and filter is in group by. Should apply constraint twice."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_fill_nulls_with_0",),
        group_by_names=("metric_time__day", "booking__is_instant"),
        where_constraint=PydanticWhereFilter(where_sql_template="{{ Dimension('booking__is_instant') }}"),
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )


@pytest.mark.sql_engine_snapshot
def test_join_to_time_spine_with_time_constraint(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Test case where metric that fills nulls is queried with a time constraint. Should apply constraint once."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_fill_nulls_with_0",),
        time_constraint_start=datetime.datetime(2020, 1, 3),
        time_constraint_end=datetime.datetime(2020, 1, 5),
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )


@pytest.mark.sql_engine_snapshot
def test_join_to_time_spine_with_queried_time_constraint(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Test case where metric that fills nulls is queried with metric time and a time constraint. Should apply constraint twice."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_fill_nulls_with_0",),
        group_by_names=("metric_time__day",),
        time_constraint_start=datetime.datetime(2020, 1, 3),
        time_constraint_end=datetime.datetime(2020, 1, 5),
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )
