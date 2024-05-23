from __future__ import annotations

import datetime

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.query_rendering.compare_rendered_query import convert_and_check


@pytest.mark.sql_engine_snapshot
def test_conversion_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test rendering a query against a conversion metric."""
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("visit_buy_conversion_rate",),
        group_by_names=("metric_time",),
        where_constraint=PydanticWhereFilter(
            where_sql_template=("{{ TimeDimension('metric_time', 'day') }} = '2020-01-01'")
        ),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(parsed_query.query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )


@pytest.mark.sql_engine_snapshot
def test_conversion_metric_with_window(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test rendering a query against a conversion metric with a window."""
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("visit_buy_conversion_rate_7days",),
        group_by_names=("metric_time",),
        where_constraint=PydanticWhereFilter(
            where_sql_template=("{{ TimeDimension('metric_time', 'day') }} = '2020-01-01'")
        ),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(parsed_query.query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )


@pytest.mark.sql_engine_snapshot
def test_conversion_metric_with_categorical_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test rendering a query against a conversion metric with a categorical filter."""
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("visit_buy_conversion_rate",),
        group_by_names=("metric_time", "visit__referrer_id"),
        where_constraint=PydanticWhereFilter(
            where_sql_template=("{{ Dimension('visit__referrer_id') }} = 'ref_id_01'")
        ),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(parsed_query.query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )


@pytest.mark.sql_engine_snapshot
def test_conversion_metric_with_time_constraint(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test rendering a query against a conversion metric with a time constraint and categorical filter."""
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("visit_buy_conversion_rate",),
        group_by_names=("visit__referrer_id",),
        where_constraint=PydanticWhereFilter(
            where_sql_template=("{{ Dimension('visit__referrer_id') }} = 'ref_id_01'")
        ),
        time_constraint_start=datetime.datetime(2020, 1, 1),
        time_constraint_end=datetime.datetime(2020, 1, 2),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(parsed_query.query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )


@pytest.mark.sql_engine_snapshot
def test_conversion_metric_with_window_and_time_constraint(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test rendering a query against a conversion metric with a window, time constraint, and categorical filter."""
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("visit_buy_conversion_rate_7days",),
        group_by_names=(
            "metric_time",
            "visit__referrer_id",
        ),
        where_constraint=PydanticWhereFilter(
            where_sql_template=("{{ Dimension('visit__referrer_id') }} = 'ref_id_01'")
        ),
        time_constraint_start=datetime.datetime(2020, 1, 1),
        time_constraint_end=datetime.datetime(2020, 1, 2),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(parsed_query.query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )
