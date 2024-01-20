from __future__ import annotations

import datetime

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.specs.specs import DimensionSpec, MetricFlowQuerySpec, TimeDimensionSpec
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.plan_conversion.test_dataflow_to_sql_plan import convert_and_check
from metricflow.test.time.metric_time_dimension import MTD_SPEC_DAY


@pytest.mark.sql_engine_snapshot
def test_metric_time_only(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests querying only metric time."""
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(
        query_spec=MetricFlowQuerySpec(
            time_dimension_specs=(TimeDimensionSpec(element_name="metric_time", entity_links=()),),
        ),
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_metric_time_quarter_alone(  # noqa:D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(
        query_spec=MetricFlowQuerySpec(
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="metric_time", entity_links=(), time_granularity=TimeGranularity.QUARTER
                ),
            ),
        ),
    )

    convert_and_check(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_metric_time_with_other_dimensions(  # noqa:D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(
        MetricFlowQuerySpec(
            time_dimension_specs=(MTD_SPEC_DAY,),
            dimension_specs=(
                DimensionSpec(element_name="home_state_latest", entity_links=(EntityReference("user"),)),
                DimensionSpec(element_name="is_lux_latest", entity_links=(EntityReference("listing"),)),
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
def test_dimensions_with_time_constraint(  # noqa:D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(
        MetricFlowQuerySpec(
            time_dimension_specs=(MTD_SPEC_DAY,),
            dimension_specs=(
                DimensionSpec(element_name="home_state_latest", entity_links=(EntityReference("user"),)),
                DimensionSpec(element_name="is_lux_latest", entity_links=(EntityReference("listing"),)),
            ),
            time_range_constraint=TimeRangeConstraint(
                start_time=datetime.datetime(2020, 1, 1), end_time=datetime.datetime(2020, 1, 3)
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
