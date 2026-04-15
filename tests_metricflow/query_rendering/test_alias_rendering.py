"""Tests rendering behavior with aliases."""

from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.query_param_implementations import (
    DimensionOrEntityParameter,
    MetricParameter,
    OrderByParameter,
    TimeDimensionParameter,
)
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.metric_time_dimension import MTD_SPEC_DAY

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.query_rendering.compare_rendered_query import render_and_check

logger = logging.getLogger(__name__)


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_simple_metric_constraint_with_single_expr_and_alias(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("double_counted_delayed_bookings",),
        group_by_names=(MTD_SPEC_DAY.dunder_name,),
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
@pytest.mark.duckdb_only
def test_aliases_with_metrics(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
) -> None:
    """Tests a metric query with various aliases."""
    metric_param = MetricParameter(name="bookings", alias="bookings_alias")
    time_dimension_param = TimeDimensionParameter(name="metric_time__day", alias="booking_day")
    dimension_param = DimensionOrEntityParameter(name="listing__capacity_latest", alias="listing_capacity")
    entity_param = DimensionOrEntityParameter(name="listing", alias="listing_id")
    query_spec = query_parser.parse_and_validate_query(
        metrics=(metric_param,),
        group_by=(time_dimension_param, dimension_param, entity_param),
        order_by=(
            OrderByParameter(metric_param),
            OrderByParameter(time_dimension_param),
            OrderByParameter(dimension_param),
            OrderByParameter(entity_param),
        ),
        where_constraint_strs=("{{ Metric('booking_fees', ['listing']) }} > 2",),
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
@pytest.mark.duckdb_only
def test_aliases_without_metrics(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
) -> None:
    """Tests a plan with an aliased dimension."""
    dimension_param = DimensionOrEntityParameter(name="listing__capacity_latest", alias="listing_capacity")
    entity_param = DimensionOrEntityParameter(name="listing", alias="listing_id")
    query_spec = query_parser.parse_and_validate_query(
        group_by=(dimension_param, entity_param),
        order_by=(OrderByParameter(dimension_param), OrderByParameter(entity_param)),
        where_constraint_strs=("{{ Dimension('listing__capacity_latest') }} > 2",),
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
@pytest.mark.duckdb_only
def test_derived_metric_alias(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
) -> None:
    """Tests a plan with an aliased metric."""
    metric = MetricParameter(name="booking_fees", alias="bookings_alias")

    query_spec = query_parser.parse_and_validate_query(
        metrics=(metric,),
        group_by_names=("metric_time__day",),
        order_by=(OrderByParameter(metric),),
        where_constraint_strs=("{{ Metric('booking_fees', ['listing']) }} > 2",),
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )
