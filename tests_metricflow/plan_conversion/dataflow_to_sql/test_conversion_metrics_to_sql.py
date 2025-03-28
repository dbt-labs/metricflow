from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.plan_conversion.test_dataflow_to_sql_plan import convert_and_check


@pytest.mark.sql_engine_snapshot
def test_conversion_rate(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Test conversion metric data flow plan rendering."""
    dimension_spec = DimensionSpec(
        element_name="referrer_id",
        entity_links=(EntityReference(element_name="visit"),),
    )
    metric_spec = MetricSpec(element_name="visit_buy_conversion_rate")

    dataflow_plan = dataflow_plan_builder.build_plan(
        query_spec=MetricFlowQuerySpec(
            metric_specs=(metric_spec,),
            dimension_specs=(dimension_spec,),
        ),
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )


@pytest.mark.sql_engine_snapshot
def test_conversion_rate_with_window(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Test conversion metric with a window data flow plan rendering."""
    dimension_spec = DimensionSpec(
        element_name="referrer_id",
        entity_links=(EntityReference(element_name="visit"),),
    )
    metric_time_spec = TimeDimensionSpec(
        element_name="metric_time",
        entity_links=(),
        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
    )
    metric_spec = MetricSpec(element_name="visit_buy_conversion_rate_7days")

    dataflow_plan = dataflow_plan_builder.build_plan(
        query_spec=MetricFlowQuerySpec(
            metric_specs=(metric_spec,),
            dimension_specs=(dimension_spec,),
            time_dimension_specs=(metric_time_spec,),
        ),
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )


@pytest.mark.sql_engine_snapshot
def test_conversion_rate_with_no_group_by(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Test conversion metric with no group by data flow plan rendering."""
    metric_spec = MetricSpec(element_name="visit_buy_conversion_rate_7days")

    dataflow_plan = dataflow_plan_builder.build_plan(
        query_spec=MetricFlowQuerySpec(
            metric_specs=(metric_spec,),
        ),
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )


@pytest.mark.sql_engine_snapshot
def test_conversion_count_with_no_group_by(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Test conversion metric with no group by data flow plan rendering."""
    metric_spec = MetricSpec(element_name="visit_buy_conversions")

    dataflow_plan = dataflow_plan_builder.build_plan(
        query_spec=MetricFlowQuerySpec(
            metric_specs=(metric_spec,),
        ),
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )


@pytest.mark.sql_engine_snapshot
def test_conversion_rate_with_constant_properties(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Test conversion metric with constant properties by data flow plan rendering."""
    metric_spec = MetricSpec(element_name="visit_buy_conversion_rate_by_session")
    dimension_spec = DimensionSpec(
        element_name="referrer_id",
        entity_links=(EntityReference(element_name="visit"),),
    )
    metric_time_spec = TimeDimensionSpec(
        element_name="metric_time",
        entity_links=(),
        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(
        query_spec=MetricFlowQuerySpec(
            metric_specs=(metric_spec,),
            dimension_specs=(dimension_spec,),
            time_dimension_specs=(metric_time_spec,),
        ),
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )


@pytest.mark.sql_engine_snapshot
def test_conversion_metric_join_to_timespine_and_fill_nulls_with_0(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Test conversion metric that joins to time spine and fills nulls with 0."""
    metric_spec = MetricSpec(element_name="visit_buy_conversion_rate_7days_fill_nulls_with_0")
    metric_time_spec = TimeDimensionSpec(
        element_name="metric_time",
        entity_links=(),
        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(
        query_spec=MetricFlowQuerySpec(
            metric_specs=(metric_spec,),
            time_dimension_specs=(metric_time_spec,),
        ),
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )
