from __future__ import annotations

import datetime

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.query_rendering.compare_rendered_query import render_and_check


@pytest.mark.sql_engine_snapshot
def test_conversion_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test rendering a query against a conversion metric."""
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("visit_buy_conversion_rate",),
        group_by_names=("metric_time",),
        where_constraints=[
            PydanticWhereFilter(where_sql_template=("{{ TimeDimension('metric_time', 'day') }} = '2020-01-01'"))
        ],
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=parsed_query.query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_conversion_metric_with_window(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test rendering a query against a conversion metric with a window."""
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("visit_buy_conversion_rate_7days",),
        group_by_names=("metric_time",),
        where_constraints=[
            PydanticWhereFilter(where_sql_template=("{{ TimeDimension('metric_time', 'day') }} = '2020-01-01'"))
        ],
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=parsed_query.query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_conversion_metric_with_categorical_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test rendering a query against a conversion metric with a categorical filter."""
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("visit_buy_conversion_rate",),
        group_by_names=("metric_time", "visit__referrer_id"),
        where_constraints=[
            PydanticWhereFilter(where_sql_template=("{{ Dimension('visit__referrer_id') }} = 'ref_id_01'"))
        ],
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=parsed_query.query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_conversion_metric_with_time_constraint(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test rendering a query against a conversion metric with a time constraint and categorical filter."""
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("visit_buy_conversion_rate",),
        group_by_names=("visit__referrer_id",),
        where_constraints=[
            PydanticWhereFilter(where_sql_template=("{{ Dimension('visit__referrer_id') }} = 'ref_id_01'"))
        ],
        time_constraint_start=datetime.datetime(2020, 1, 1),
        time_constraint_end=datetime.datetime(2020, 1, 2),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=parsed_query.query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_conversion_metric_with_window_and_time_constraint(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
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
        where_constraints=[
            PydanticWhereFilter(where_sql_template=("{{ Dimension('visit__referrer_id') }} = 'ref_id_01'"))
        ],
        time_constraint_start=datetime.datetime(2020, 1, 1),
        time_constraint_end=datetime.datetime(2020, 1, 2),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=parsed_query.query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_conversion_metric_with_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test rendering a query against a conversion metric."""
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("visit_buy_conversion_rate",),
        where_constraints=(
            PydanticWhereFilter(where_sql_template=("{{ TimeDimension('metric_time', 'day') }} = '2020-01-01'")),
        ),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=parsed_query.query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_conversion_metric_with_filter_not_in_group_by(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test rendering a query against a conversion metric."""
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("visit_buy_conversions",),
        where_constraints=(
            PydanticWhereFilter(where_sql_template=("{{ Dimension('visit__referrer_id') }} = 'ref_id_01'")),
        ),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=parsed_query.query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_conversion_metric_with_different_time_dimension_grains(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test rendering a query against a conversion metric."""
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("visit_buy_conversion_rate_with_monthly_conversion",),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=parsed_query.query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_conversion_metric_with_metric_definition_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
    create_source_tables: bool,
) -> None:
    """Test rendering a query against a conversion metric with a filter defined in the YAML metric definition."""
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("visit_buy_conversion_rate_with_filter",),
        group_by_names=("metric_time__day",),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=parsed_query.query_spec,
    )
