"""Test how different filters are applied to time-offset metrics."""

from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from metricflow_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from tests_metricflow.query_rendering.compare_rendered_query import render_and_check

logger = logging.getLogger(__name__)


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_metric_with_metric_time_filter(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    create_source_tables: bool,
) -> None:
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_offset_once",),
        group_by_names=(METRIC_TIME_ELEMENT_NAME,),
        where_constraints=[
            PydanticWhereFilter(where_sql_template=("{{ TimeDimension('metric_time', 'day') }} = '2020-01-01' "))
        ],
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
        expectation_description=(
            "The metric_time filter should be applied on the time spine / output side "
            "of the offset join, ideally by pushing it to the time spine before the join "
            "rather than into the pre-offset metric input."
        ),
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_metric_with_dimension_filter(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    create_source_tables: bool,
) -> None:
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_offset_once",),
        group_by_names=(METRIC_TIME_ELEMENT_NAME,),
        where_constraints=[
            PydanticWhereFilter(where_sql_template=("{{ Dimension('listing__country_latest') }} == 'us'"))
        ],
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
        expectation_description=(
            "The dimension filter should stay on the metric input before the offset join "
            "so it constrains the source rows that are being shifted."
        ),
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_metric_with_metric_time_and_dimension_filter(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    create_source_tables: bool,
) -> None:
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_offset_once",),
        group_by_names=(METRIC_TIME_ELEMENT_NAME,),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template=(
                    "{{ TimeDimension('metric_time', 'day') }} = '2020-01-01'"
                    " AND {{ Dimension('listing__country_latest') }} == 'us'"
                )
            )
        ],
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
        expectation_description=(
            "The metric_time portion of the filter should be applied on the time "
            "spine / output side of the offset join, ideally by pushing it to the "
            "time spine before the join, while the dimension portion should stay on "
            "the pre-offset metric input."
        ),
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_metric_with_separate_metric_time_and_dimension_filters(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    create_source_tables: bool,
) -> None:
    """Test querying a time-offset metric with separate filters that allow for different filter placement."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_offset_once",),
        group_by_names=(METRIC_TIME_ELEMENT_NAME,),
        where_constraint_strs=[
            "{{ TimeDimension('metric_time', 'day') }} = '2020-01-01'",
            "{{ Dimension('listing__country_latest') }} == 'us'",
        ],
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
        expectation_description=(
            "The metric_time portion of the filter (`{{ TimeDimension('metric_time', 'day') }} = '2020-01-01'`) "
            "should be applied on the time spine / output side of the offset join, ideally by pushing it to the "
            "time spine before the join, while the dimension portion "
            "(`{{ Dimension('listing__country_latest') }} == 'us'`) should stay on the pre-offset metric input."
        ),
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_cumulative_metric_with_metric_time_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
) -> None:
    """Tests querying a cumulative metric that is offset with a filter on metric time."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("trailing_7_days_bookings_offset_1_week",),
        group_by_names=("metric_time__day",),
        where_constraint_strs=("{{ TimeDimension('metric_time', 'day') }} = '2020-01-01'",),
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
        expectation_description=(
            "The metric_time filter should be applied on the time spine / output side "
            "of the cumulative offset, ideally by pushing it to the time spine "
            "before the join rather than inside the pre-offset cumulative input."
        ),
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_metric_with_string_filter(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    create_source_tables: bool,
) -> None:
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_offset_once",),
        group_by_names=(METRIC_TIME_ELEMENT_NAME,),
        where_constraints=[PydanticWhereFilter(where_sql_template=("TRUE"))],
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
        expectation_description=(
            "This test uses TRUE as a placeholder for arbitrary opaque SQL. "
            "Opaque SQL predicates are not analyzed for safe splitting and are "
            "assumed not to reference aggregation time dimensions. The current "
            "expectation is that the filter should be pushed to the pre-aggregation "
            "branch. However, the appropriate behavior may be to not push at all."
        ),
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_nested_offset_metric_with_non_queried_element_in_filter(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    create_source_tables: bool,
) -> None:
    """Tests that a non-queried filter element does not remain in the aggregation grain."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings_offset_twice",),
        group_by_names=(METRIC_TIME_ELEMENT_NAME,),
        where_constraint_strs=["{{ Entity('listing') }} = '1'"],
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
        expectation_description=(
            "The non-queried listing filter should be available for filtering the "
            "nested offset metric, but it should not remain part of the aggregation "
            "grain after filtering. The current snapshot does not reflect the "
            "correct result."
        ),
    )
