from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.query_rendering.compare_rendered_query import render_and_check


@pytest.mark.sql_engine_snapshot
def test_metric_filter_with_metric_time_day(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a metric filter that includes metric_time in group_by with day granularity."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("active_listings",),
        group_by_names=("metric_time__day",),
        where_constraints=[],
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_metric_filter_with_metric_time_month(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a metric filter that includes metric_time in group_by with month granularity."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("active_listings",),
        group_by_names=("metric_time__month",),
        where_constraints=[],
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_metric_filter_with_metric_time_in_where_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a metric filter in the where clause that includes metric_time in group_by."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        group_by_names=("metric_time__month",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Metric('bookings', ['listing']) }} > 0",
            )
        ],
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_metric_filter_with_metric_time_day_granularity(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a metric filter that includes metric_time in group_by with day granularity."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        group_by_names=("metric_time__day",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Metric('bookings', ['listing']) }} > 0",
            )
        ],
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_metric_filter_with_metric_time_month_granularity(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a metric filter that includes metric_time in group_by with month granularity."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        group_by_names=("metric_time__month",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Metric('bookings', ['listing']) }} > 0",
            )
        ],
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )
