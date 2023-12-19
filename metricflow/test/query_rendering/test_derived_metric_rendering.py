"""Tests derived metric query rendering by comparing rendered output against snapshot files."""

from __future__ import annotations

import datetime

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.naming.dunder_scheme import DunderNamingScheme
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.query.query_parser import MetricFlowQueryParser
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.specs import (
    MetricFlowQuerySpec,
    MetricSpec,
)
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.query_rendering.compare_rendered_query import convert_and_check
from metricflow.test.time.metric_time_dimension import (
    MTD_SPEC_DAY,
    MTD_SPEC_MONTH,
    MTD_SPEC_QUARTER,
    MTD_SPEC_WEEK,
    MTD_SPEC_YEAR,
)


@pytest.mark.sql_engine_snapshot
def test_derived_metric(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="non_referred_bookings_pct"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_nested_derived_metric(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="instant_plus_non_referred_bookings_pct"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_metric_with_offset_window(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_growth_2_weeks"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_metric_with_offset_window_and_time_filter(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    column_association_resolver: ColumnAssociationResolver,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_growth_2_weeks",),
        group_by_names=(METRIC_TIME_ELEMENT_NAME,),
        where_constraint=PydanticWhereFilter(
            where_sql_template=(
                "{{ TimeDimension('metric_time', 'day') }} = '2020-01-01' "
                "or {{ TimeDimension('metric_time', 'day') }} = '2020-01-14'"
            )
        ),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_metric_with_offset_to_grain(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_growth_since_start_of_month"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_metric_with_offset_window_and_offset_to_grain(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_month_start_compared_to_1_month_prior"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_offset_metric_with_one_input_metric(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_5_day_lag"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_metric_with_offset_window_and_granularity(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_growth_2_weeks"),),
            time_dimension_specs=(MTD_SPEC_QUARTER,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_metric_with_month_dimension_and_offset_window(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    extended_date_dataflow_plan_builder: DataflowPlanBuilder,
    extended_date_dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = extended_date_dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_last_month"),),
            time_dimension_specs=(MTD_SPEC_MONTH,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=extended_date_dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_metric_with_offset_to_grain_and_granularity(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_growth_since_start_of_month"),),
            time_dimension_specs=(MTD_SPEC_WEEK,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_metric_with_offset_window_and_offset_to_grain_and_granularity(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_month_start_compared_to_1_month_prior"),),
            time_dimension_specs=(MTD_SPEC_YEAR,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_derived_offset_cumulative_metric(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="every_2_days_bookers_2_days_ago"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_nested_offsets(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
    create_source_tables: bool,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_offset_twice"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_nested_derived_metric_with_offset_multiple_input_metrics(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
    create_source_tables: bool,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="booking_fees_since_start_of_month"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_nested_offsets_with_where_constraint(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
    column_association_resolver: ColumnAssociationResolver,
    create_source_tables: bool,
) -> None:
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_offset_twice",),
        group_by_names=(METRIC_TIME_ELEMENT_NAME,),
        where_constraint=PydanticWhereFilter(
            where_sql_template=(
                "{{ TimeDimension('metric_time', 'day') }} = '2020-01-12' "
                "or {{ TimeDimension('metric_time', 'day') }} = '2020-01-13'"
            )
        ),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_nested_offsets_with_time_constraint(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
    create_source_tables: bool,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_offset_twice"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
            time_range_constraint=TimeRangeConstraint(
                start_time=datetime.datetime(2020, 1, 12), end_time=datetime.datetime(2020, 1, 13)
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_time_offset_metric_with_time_constraint(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
    create_source_tables: bool,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_5_day_lag"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
            time_range_constraint=TimeRangeConstraint(
                start_time=datetime.datetime(2019, 12, 19), end_time=datetime.datetime(2020, 1, 2)
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_cumulative_time_offset_metric_with_time_constraint(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
    create_source_tables: bool,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        query_spec=MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="every_2_days_bookers_2_days_ago"),),
            time_dimension_specs=(MTD_SPEC_DAY,),
            time_range_constraint=TimeRangeConstraint(
                start_time=datetime.datetime(2019, 12, 19), end_time=datetime.datetime(2020, 1, 2)
            ),
        )
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_nested_derived_metric_offset_with_joined_where_constraint_not_selected(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
    create_source_tables: bool,
    column_association_resolver: ColumnAssociationResolver,
) -> None:
    group_by_name = DunderNamingScheme().input_str(MTD_SPEC_DAY)
    assert group_by_name is not None

    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_offset_twice",),
        group_by_names=(group_by_name,),
        where_constraint_str="{{ Dimension('booking__is_instant') }}",
    )

    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)
    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )
