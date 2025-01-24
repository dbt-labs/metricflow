from __future__ import annotations

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
def test_query_with_simple_metric_in_where_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a simple metric in the query-level where filter."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Metric('bookings', ['listing']) }} > 2",
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
    )


@pytest.mark.sql_engine_snapshot
def test_metric_with_metric_in_where_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a metric in the metric-level where filter."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("active_listings",),
        group_by_names=("metric_time__day",),
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_query_with_derived_metric_in_where_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a derived metric in the query-level where filter."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Metric('views_times_booking_value', ['listing']) }} > 1",
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
    )


@pytest.mark.sql_engine_snapshot
def test_query_with_ratio_metric_in_where_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a ratio metric in the query-level where filter."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Metric('bookings_per_booker', ['listing']) }} > 1",
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
    )


@pytest.mark.sql_engine_snapshot
def test_query_with_cumulative_metric_in_where_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a cumulative metric in the query-level where filter.

    Note this cumulative metric has no window / grain to date.
    """
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Metric('revenue_all_time', ['user']) }} > 1",
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
    )


@pytest.mark.sql_engine_snapshot
def test_query_with_multiple_metrics_in_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with 2 simple metrics in the query-level where filter."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Metric('bookings', ['listing']) }} > 2 AND {{ Metric('bookers', ['listing']) }} > 1",
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
    )


@pytest.mark.sql_engine_snapshot
def test_filter_by_metric_in_same_semantic_model_as_queried_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a simple metric in the query-level where filter."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookers",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Metric('booking_value', ['guest']) }} > 1.00",
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
    )


@pytest.mark.sql_engine_snapshot
def test_distinct_values_query_with_metric_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a distinct values query with a metric in the query-level where filter."""
    query_spec = query_parser.parse_and_validate_query(
        group_by_names=("listing",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Metric('bookings', ['listing']) }} > 2",
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
    )


@pytest.mark.sql_engine_snapshot
def test_metric_filtered_by_itself(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query for a metric that filters by the same metric."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookers",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Metric('bookers', ['listing']) }} > 1.00",
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
    )


@pytest.mark.sql_engine_snapshot
def test_group_by_has_local_entity_prefix(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Metric('average_booking_value', ['listing__user']) }} > 1",
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
    )


@pytest.mark.sql_engine_snapshot
def test_filter_with_conversion_metric(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Metric('visit_buy_conversion_rate', ['user']) }} > 2",
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
    )


@pytest.mark.sql_engine_snapshot
def test_inner_query_single_hop(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    multihop_dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    multihop_dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    multihop_query_parser: MetricFlowQueryParser,
) -> None:
    """Tests rendering for a metric filter using a one-hop join in the inner query."""
    query_spec = multihop_query_parser.parse_and_validate_query(
        metric_names=("third_hop_count",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Metric('paraguayan_customers', ['customer_id__customer_third_hop_id']) }} > 0",
            )
        ],
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=multihop_dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=multihop_dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_inner_query_multi_hop(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    multihop_dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    multihop_dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    multihop_query_parser: MetricFlowQueryParser,
) -> None:
    """Tests rendering for a metric filter using a two-hop join in the inner query."""
    query_spec = multihop_query_parser.parse_and_validate_query(
        metric_names=("third_hop_count",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Metric('txn_count', ['account_id__customer_id__customer_third_hop_id']) }} > 2",
            )
        ],
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=multihop_dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=multihop_dataflow_plan_builder,
        query_spec=query_spec,
    )
