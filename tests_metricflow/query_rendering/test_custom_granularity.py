"""Tests metric query rendering for granularity and date part operations.

This module runs query requests for various granularity/date part options and compares
the rendered output against snapshot files.
"""

from __future__ import annotations

import datetime as dt

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataset.dataset_classes import DataSet
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


# - Optimization: subquery could be collapsed. Likely unrelated to this feature
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


# - Optimization: subqueries could be collapsed. May be unrelated to this feature
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


# - Optimization: subqueries could be collapsed. May be unrelated to this feature
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


@pytest.mark.sql_engine_snapshot
def test_no_metric_custom_granularity_non_metric_time(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = MetricFlowQuerySpec(
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


# Tests with issues below!


# - Bug: alias used but never defined. Appears to be happening in optimization. subq_0
# - Bug: unoptimized query shows duplicate of the same column in the source node
# TODO: make sure the column pruner error removal needed for this one is fine (ask Tom) - see if you can add it back at the end perhaps
@pytest.mark.sql_engine_snapshot
def test_no_metric_custom_granularity_joined_to_non_default_grain(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = MetricFlowQuerySpec(
        time_dimension_specs=(
            DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY),
            metric_time_with_custom_grain,
            normal_time_dim_with_custom_grain2,
            TimeDimensionSpec(
                element_name="bio_added_ts",
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH),
                entity_links=(EntityReference("user"),),
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


# - Bug: subq_3 alias is used but never defined. Appears to be happening in optimization.
# - Optimization: we could just select directly from the time spine, but istead we join one time spine to another.
# - Optimization: we start by selecting from the time spine with second grain, when it would be more efficient to select
# from the time spine with day. Not related to this feature, but should fix.
@pytest.mark.sql_engine_snapshot
def test_no_metric_custom_granularity_metric_time(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = MetricFlowQuerySpec(
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


# TODO: add more tests
# - with multiple custom granularities
# - with custom granularity that has a different name than its column name
# - with every type of dataflow plan that uses join to time spine node
# - with cumulative metrics
# - query parsing tests - mapping from custom granularity name to time dimension spec not handled yet
# - check query tests

# Filter tests, when supported:
# @pytest.mark.sql_engine_snapshot
# def test_no_metric_query_with_custom_granularity_filters_included_in_group_by(  # noqa: D103
#     request: FixtureRequest,
#     mf_test_configuration: MetricFlowTestConfiguration,
#     dataflow_plan_builder: DataflowPlanBuilder,
#     dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
#     sql_client: SqlClient,
# ) -> None:
#     query_spec = MetricFlowQuerySpec(
#         # A no-metric query, where filter on metric_time with custom grain, included in group by
# #    Also a time constraint on the same dimension
#         time_dimension_specs=(metric_time_with_custom_grain,),
#         where_constraint=PydanticWhereFilter(
#             where_sql_template="{{ TimeDimension('metric_time') }} > '2020-01-01'",
#         ),
#         time_constraint_start=datetime.datetime(2020, 1, 3),
#         time_constraint_end=datetime.datetime(2020, 1, 5),
#     )

#     render_and_check(
#         request=request,
#         mf_test_configuration=mf_test_configuration,
#         dataflow_to_sql_converter=dataflow_to_sql_converter,
#         sql_client=sql_client,
#         dataflow_plan_builder=dataflow_plan_builder,
#         query_spec=query_spec,
#     )


# @pytest.mark.sql_engine_snapshot
# def test_no_metric_query_with_custom_granularity_filters_not_included_in_group_by(  # noqa: D103
#     request: FixtureRequest,
#     mf_test_configuration: MetricFlowTestConfiguration,
#     dataflow_plan_builder: DataflowPlanBuilder,
#     dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
#     sql_client: SqlClient,
# ) -> None:
#     query_spec = MetricFlowQuerySpec(
#         # TODO: no-metric query, where filter on non-metric_time with custom grain, not included in group by, also another time dim with standard grain
#         # Also a time constraint not included in group by
#         time_dimension_specs=(DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY),),
#     )

#     render_and_check(
#         request=request,
#         mf_test_configuration=mf_test_configuration,
#         dataflow_to_sql_converter=dataflow_to_sql_converter,
#         sql_client=sql_client,
#         dataflow_plan_builder=dataflow_plan_builder,
#         query_spec=query_spec,
#     )
# @pytest.mark.sql_engine_snapshot
# def test_metric_query_with_custom_granularity_filters_included_in_group_by(  # noqa: D103
#     request: FixtureRequest,
#     mf_test_configuration: MetricFlowTestConfiguration,
#     dataflow_plan_builder: DataflowPlanBuilder,
#     dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
#     sql_client: SqlClient,
# ) -> None:
#     query_spec = MetricFlowQuerySpec(
#         # TODO: metric query, where filter on metric_time with custom grain, included in group by
#         # Also a time constraint on the same dimension
#         time_dimension_specs=(DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY),),
#     )

#     render_and_check(
#         request=request,
#         mf_test_configuration=mf_test_configuration,
#         dataflow_to_sql_converter=dataflow_to_sql_converter,
#         sql_client=sql_client,
#         dataflow_plan_builder=dataflow_plan_builder,
#         query_spec=query_spec,
#     )


# @pytest.mark.sql_engine_snapshot
# def test_metric_query_with_custom_granularity_filters_not_included_in_group_by(  # noqa: D103
#     request: FixtureRequest,
#     mf_test_configuration: MetricFlowTestConfiguration,
#     dataflow_plan_builder: DataflowPlanBuilder,
#     dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
#     sql_client: SqlClient,
# ) -> None:
#     query_spec = MetricFlowQuerySpec(
#         # TODO: metric query, where filter on non-metric_time with custom grain, not included in group by, also another time dim with standard grain
#         # Also a time constraint not included in group by
#         time_dimension_specs=(DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY),),
#     )

#     render_and_check(
#         request=request,
#         mf_test_configuration=mf_test_configuration,
#         dataflow_to_sql_converter=dataflow_to_sql_converter,
#         sql_client=sql_client,
#         dataflow_plan_builder=dataflow_plan_builder,
#         query_spec=query_spec,
#     )
