from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.query.query_parser import MetricFlowQueryParser
from tests.fixtures.setup_fixtures import MetricFlowTestConfiguration
from tests.query_rendering.compare_rendered_query import convert_and_check


@pytest.mark.sql_engine_snapshot
def test_query_with_simple_metric_in_where_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a simple metric in the query-level where filter."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        where_constraint=PydanticWhereFilter(
            where_sql_template="{{ Metric('bookings', ['listing']) }} > 2",
        ),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_metric_with_metric_in_where_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a metric in the metric-level where filter."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("active_listings",),
        group_by_names=("metric_time__day",),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_query_with_derived_metric_in_where_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a derived metric in the query-level where filter."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        where_constraint=PydanticWhereFilter(
            where_sql_template="{{ Metric('views_times_booking_value', ['listing']) }} > 1",
        ),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_query_with_ratio_metric_in_where_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a ratio metric in the query-level where filter."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        where_constraint=PydanticWhereFilter(
            where_sql_template="{{ Metric('bookings_per_booker', ['listing']) }} > 1",
        ),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_query_with_cumulative_metric_in_where_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a cumulative metric in the query-level where filter.

    Note this cumulative metric has no window / grain to date.
    """
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        where_constraint=PydanticWhereFilter(
            where_sql_template="{{ Metric('revenue_all_time', ['user']) }} > 1",
        ),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_query_with_multiple_metrics_in_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with 2 simple metrics in the query-level where filter."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        where_constraint=PydanticWhereFilter(
            where_sql_template="{{ Metric('bookings', ['listing']) }} > 2 AND {{ Metric('bookers', ['listing']) }} > 1",
        ),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


# TODO: tests for filters with conversion metrics + cumulative metrics with window/grain, distinct values queries with metric filters
