"""Tests for metric filter time granularity inheritance.

This module tests the behavior of metric filters with respect to time granularity inheritance.
"""

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
def test_metric_filter_without_explicit_metric_time(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a metric filter that does not include metric_time in its group_by list.

    This test verifies that time granularity inheritance does not happen when metric_time is not
    explicitly included in the filter's group_by list, even if the parent query has a time granularity.
    """
    # We need to use a metric that exists in the test data
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings",),
        group_by_names=("metric_time__month",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Metric('listings', ['listing']) }} > 0",
            )
        ],
    ).query_spec

    # Generate the snapshot for this test
    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_metric_filter_with_different_time_granularity(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a metric filter where the parent query has a different time granularity.

    This test verifies that the parent query's time granularity is respected even when the filter
    doesn't explicitly include metric_time in its group_by list.
    """
    # We need to use a metric that exists in the test data
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings",),
        group_by_names=("metric_time__day",),  # Day granularity
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Metric('listings', ['listing']) }} > 0",
            )
        ],
    ).query_spec

    # Generate the snapshot for this test
    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )
