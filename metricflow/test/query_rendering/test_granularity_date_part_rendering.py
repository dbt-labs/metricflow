"""Tests metric query rendering for granularity and date part operations.

This module runs query requests for various granularity/date part options and compares
the rendered output against snapshot files.
"""

from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataset.dataset import DataSet
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.specs.specs import (
    MetricFlowQuerySpec,
    MetricSpec,
)
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.query_rendering.compare_rendered_query import convert_and_check


@pytest.mark.sql_engine_snapshot
def test_simple_query_with_date_part(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            time_dimension_specs=(
                DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY, date_part=DatePart.DOW),
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
def test_simple_query_with_multiple_date_parts(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            time_dimension_specs=(
                DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY, date_part=DatePart.DAY),
                DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY, date_part=DatePart.DOW),
                DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY, date_part=DatePart.DOY),
                DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY, date_part=DatePart.MONTH),
                DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY, date_part=DatePart.QUARTER),
                DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY, date_part=DatePart.YEAR),
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
def test_offset_window_with_date_part(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_growth_2_weeks"),),
            time_dimension_specs=(
                DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY, date_part=DatePart.DOW),
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
