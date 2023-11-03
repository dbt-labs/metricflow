"""Tests for rendering queries requiring joins against a time spine dataset.

This module is specifically for query rendering, rather than time spine join node behavior. It is
meant to encompass both the basis of the null coalesce time spine join rendering configuration
and other time-spine related features, such as exploding a time range aggregation into a set of
rows covering each sub-interval in the range (e.g., for measure defined in SCD datasets)
"""

from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
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
def test_simple_join_to_time_spine(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
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
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )
