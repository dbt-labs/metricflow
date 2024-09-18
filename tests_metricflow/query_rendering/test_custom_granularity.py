"""Tests metric query rendering for granularity and date part operations.

This module runs query requests for various granularity/date part options and compares
the rendered output against snapshot files.
"""

from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.query_rendering.compare_rendered_query import render_and_check

metric_time_with_custom_grain = TimeDimensionSpec(
    "metric_time",
    entity_links=(),
    time_granularity=ExpandedTimeGranularity(name="martian_day", base_granularity=TimeGranularity.DAY),
)
normal_time_dim_with_custom_grain1 = TimeDimensionSpec(
    element_name="ds",
    time_granularity=ExpandedTimeGranularity(name="martian_day", base_granularity=TimeGranularity.DAY),
    entity_links=(EntityReference("booking"),),
)
normal_time_dim_with_custom_grain2 = TimeDimensionSpec(
    element_name="bio_added_ts",
    time_granularity=ExpandedTimeGranularity(name="martian_day", base_granularity=TimeGranularity.DAY),
    entity_links=(EntityReference("user"),),
)


# TODO: subqueries in this test should be collapsed. Update optimizer
@pytest.mark.sql_engine_snapshot
def test_simple_metric_with_custom_granularity(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec("bookings"),),
        time_dimension_specs=(normal_time_dim_with_custom_grain1,),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_cumulative_metric_with_custom_granularity(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec("trailing_2_months_revenue"),),
        time_dimension_specs=(metric_time_with_custom_grain,),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_metric_with_custom_granularity(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec("booking_fees_per_booker"),),
        time_dimension_specs=(normal_time_dim_with_custom_grain1,),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


# TODO: subqueries in this test should be collapsed. Update optimizer
@pytest.mark.sql_engine_snapshot
def test_multiple_metrics_with_custom_granularity(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec("bookings"), MetricSpec("listings")),
        time_dimension_specs=(metric_time_with_custom_grain,),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


# TODO: subqueries in this test should be collapsed. Update optimizer
@pytest.mark.sql_engine_snapshot
def test_metric_custom_granularity_joined_to_non_default_grain(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec("listings"),),
        time_dimension_specs=(
            metric_time_with_custom_grain,
            TimeDimensionSpec(
                element_name="ds",
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH),
                entity_links=(EntityReference("listing"),),
            ),
        ),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )
