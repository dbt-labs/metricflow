from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.query_param_implementations import SavedQueryParameter
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.query_rendering.compare_rendered_query import render_and_check


@pytest.mark.sql_engine_snapshot
def test_single_categorical_dimension_pushdown(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query where we expect predicate pushdown for a single categorical dimension."""
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("bookings",),
        group_by_names=("listing__country_latest",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Dimension('booking__is_instant') }}",
            )
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
def test_multiple_categorical_dimension_pushdown(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query where we expect predicate pushdown for more than one categorical dimension."""
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        group_by_names=("user__home_state_latest",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Dimension('listing__is_lux_latest') }} OR {{ Dimension('listing__capacity_latest') }} > 4",
            )
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
def test_different_filters_on_same_simple_metric_source_categorical_dimension(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query where multiple filters against the same simple-metric input dimension need to be an effective OR.

    This can be an issue where a derived metric takes in two filters that refer to the same dimension from the input
    measure source. If these filters are disjoint the predicate pushdown needs to ensure that all matching rows are
    returned, so we cannot simply push one filter or the other down, nor can we push them down as an AND - they
    must be an OR, since all relevant rows need to be returned to the requesting metrics.

    The metric listed here has one input that filters on bookings__is_instant and another that does not, which means
    the source input for the latter input must NOT have the filter applied to it.
    """
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("instant_booking_fraction_of_max_value",),
        group_by_names=("metric_time",),
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
def test_skipped_pushdown(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query where we expect to skip predicate pushdown because it is unsafe.

    This is the query rendering test for the scenarios where the push down evaluation indicates that we should
    skip pushdown, typically due to a lack of certainty over whether or not the query will return the same results.

    The specific scenario is less important here than that it match one that should not be pushed down.
    """
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("bookings",),
        group_by_names=("listing__country_latest",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Dimension('booking__is_instant') }} OR {{ Dimension('listing__is_lux_latest') }}",
            )
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
def test_metric_time_filter_with_two_targets(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests pushdown optimization for a simple metric time predicate through a single join.

    This is currently a no-op for the pushdown optimizer.
    TODO: support metric time pushdown

    """
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("bookings",),
        group_by_names=("listing__country_latest",),
        where_constraints=[PydanticWhereFilter(where_sql_template="{{ TimeDimension('metric_time') }} = '2024-01-01'")],
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
def test_conversion_metric_query_filters(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests pushdown optimizer behavior for a simple predicate on a conversion metric."""
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("visit_buy_conversion_rate_7days",),
        group_by_names=("metric_time", "user__home_state_latest"),
        where_constraints=[PydanticWhereFilter(where_sql_template="{{ Dimension('visit__referrer_id') }} = '123456'")],
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
def test_cumulative_metric_with_query_time_filters(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests pushdown optimizer behavior for a query against a cumulative metric.

    TODO: support metric time filters
    """
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("every_two_days_bookers",),
        group_by_names=("listing__country_latest", "metric_time"),
        where_constraints=[PydanticWhereFilter(where_sql_template="{{ Dimension('booking__is_instant') }}")],
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
def test_offset_metric_with_query_time_filters(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests pushdown optimizer behavior for a query against a derived offset metric.

    TODO: support metric time filters
    """
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("bookings_growth_2_weeks",),
        group_by_names=("listing__country_latest", "metric_time"),
        where_constraints=[PydanticWhereFilter(where_sql_template="{{ Dimension('booking__is_instant') }}")],
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
def test_fill_nulls_time_spine_metric_predicate_pushdown(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests pushdown optimizer behavior for a metric with a time spine and fill_nulls_with enabled.

    TODO: support metric time filters
    """
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("bookings_growth_2_weeks_fill_nulls_with_0",),
        group_by_names=("listing__country_latest", "metric_time"),
        where_constraints=[PydanticWhereFilter(where_sql_template="{{ Dimension('booking__is_instant') }}")],
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
def test_simple_join_to_time_spine_pushdown_filter_application(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query where we join to a time spine and query the filter input.

    This should produce a SQL query that applies the filter outside of the time spine join.
    """
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("bookings_join_to_time_spine",),
        group_by_names=("booking__is_instant", "metric_time"),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Dimension('booking__is_instant') }}",
            )
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
def test_saved_query_with_metric_joins_and_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query where we join to a time spine and query the filter input.

    This should produce a SQL query that applies the filter outside of the time spine join.
    """
    parsed_query = query_parser.parse_and_validate_saved_query(
        saved_query_parameter=SavedQueryParameter("saved_query_with_metric_joins_and_filter"),
        where_filters=None,
        limit=None,
        time_constraint_start=None,
        time_constraint_end=None,
        order_by_names=None,
        order_by_parameters=None,
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=parsed_query.query_spec,
    )
