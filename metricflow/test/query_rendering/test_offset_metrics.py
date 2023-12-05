from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.test_utils import as_datetime
from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataset.dataset import DataSet
from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.specs.specs import MetricFlowQuerySpec, MetricSpec
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.plan_conversion.test_dataflow_to_sql_plan import convert_and_check


@pytest.mark.sql_engine_snapshot
def test_offset_metric_with_time_constraint(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests converting a PlotTimeDimensionTransform node using the primary time dimension to SQL."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings_2_weeks_ago"),),
            time_dimension_specs=(DataSet.metric_time_dimension_spec(time_granularity=TimeGranularity.DAY),),
            time_range_constraint=TimeRangeConstraint(
                start_time=as_datetime("2020-01-01"),
                end_time=as_datetime("2020-01-01"),
            )
        )
    )
    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_node.parent_node,
    )
