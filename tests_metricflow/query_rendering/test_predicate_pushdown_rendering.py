from __future__ import annotations

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
def test_single_categorical_dimension_pushdown(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query where we expect predicate pushdown for a single categorical dimension."""
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("bookings",),
        group_by_names=("listing__country_latest",),
        where_constraint=PydanticWhereFilter(
            where_sql_template="{{ Dimension('booking__is_instant') }}",
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
def test_multiple_categorical_dimension_pushdown(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query where we expect predicate pushdown for more than one categorical dimension."""
    parsed_query = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        group_by_names=("user__home_state_latest",),
        where_constraint=PydanticWhereFilter(
            where_sql_template="{{ Dimension('listing__is_lux_latest') }} OR {{ Dimension('listing__capacity_latest') }} > 4",
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
def test_different_filters_on_same_measure_source_categorical_dimension(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests rendering a query where multiple filters against the same measure dimension need to be an effective OR.

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
    dataflow_plan = dataflow_plan_builder.build_plan(parsed_query.query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )


@pytest.mark.sql_engine_snapshot
def test_skipped_pushdown(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
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
        where_constraint=PydanticWhereFilter(
            where_sql_template="{{ Dimension('booking__is_instant') }} OR {{ Dimension('listing__is_lux_latest') }}",
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
