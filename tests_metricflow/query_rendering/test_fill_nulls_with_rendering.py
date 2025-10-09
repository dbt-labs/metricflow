"""Tests query rendering for coalescing null simple-metric inputs by comparing rendered output against snapshot files."""

from __future__ import annotations

import datetime

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
)
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.metric_time_dimension import MTD_SPEC_DAY, MTD_SPEC_MONTH
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.query_rendering.compare_rendered_query import render_and_check


@pytest.mark.sql_engine_snapshot
def test_simple_fill_nulls_with_0_metric_time(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="bookings_fill_nulls_with_0"),),
        time_dimension_specs=(MTD_SPEC_DAY,),
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
def test_simple_fill_nulls_with_0_month(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="bookings_fill_nulls_with_0"),),
        time_dimension_specs=(MTD_SPEC_MONTH,),
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
def test_simple_fill_nulls_with_0_with_non_metric_time(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="bookings_fill_nulls_with_0"),),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="paid_at",
                entity_links=(EntityReference("booking"),),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
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
def test_simple_fill_nulls_with_0_with_categorical_dimension(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="bookings_fill_nulls_with_0"),),
        dimension_specs=(DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),),
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
def test_simple_fill_nulls_without_time_spine(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="bookings_fill_nulls_with_0_without_time_spine"),),
        time_dimension_specs=(MTD_SPEC_DAY,),
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
def test_cumulative_fill_nulls(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="every_two_days_bookers_fill_nulls_with_0"),),
        time_dimension_specs=(MTD_SPEC_DAY,),
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
def test_derived_fill_nulls_for_one_input_metric(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="bookings_growth_2_weeks_fill_nulls_with_0_for_non_offset"),),
        time_dimension_specs=(MTD_SPEC_DAY,),
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
def test_join_to_time_spine_with_filters(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
) -> None:
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_fill_nulls_with_0",),
        group_by_names=("metric_time__day",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ TimeDimension('metric_time') }} > '2020-01-01'",
            )
        ],
        time_constraint_start=datetime.datetime(2020, 1, 3),
        time_constraint_end=datetime.datetime(2020, 1, 5),
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
def test_join_to_time_spine_with_filter_not_in_group_by(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
) -> None:
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_join_to_time_spine_with_tiered_filters",),
        group_by_names=("metric_time__day",),
        where_constraints=[
            PydanticWhereFilter(where_sql_template="{{ TimeDimension('metric_time', 'month') }} > '2020-01-01'")
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
def test_join_to_time_spine_with_filter_smaller_than_group_by(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
) -> None:
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("archived_users_join_to_time_spine",),
        group_by_names=("metric_time__day",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ TimeDimension('metric_time', 'hour') }} > '2020-01-01 00:09:00'"
            ),
            PydanticWhereFilter(where_sql_template="{{ TimeDimension('metric_time', 'day') }} = '2020-01-01'"),
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
def test_join_to_time_spine_with_filter_not_in_group_by_using_agg_time(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
) -> None:
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_join_to_time_spine_with_tiered_filters",),
        group_by_names=("booking__ds__day",),
        where_constraints=[
            PydanticWhereFilter(where_sql_template="{{ TimeDimension('booking__ds', 'month') }} > '2020-01-01'")
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
def test_join_to_time_spine_with_filter_not_in_group_by_using_agg_time_and_metric_time(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
) -> None:
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_join_to_time_spine_with_tiered_filters",),
        group_by_names=("metric_time__day",),
        where_constraints=[
            PydanticWhereFilter(where_sql_template="{{ TimeDimension('booking__ds', 'month') }} > '2020-01-01'")
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
