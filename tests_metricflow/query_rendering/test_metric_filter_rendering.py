from __future__ import annotations

import datetime

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.dag.mf_dag import DagId
from metricflow_semantics.errors.error_classes import InvalidQueryException
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.optimizer.optimization_levels import SqlOptimizationLevel
from metricflow_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
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


def _render_query_sql(
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    query_spec: MetricFlowQuerySpec,
) -> str:
    plan = dataflow_plan_builder.build_plan(query_spec)
    conversion_result = dataflow_to_sql_converter.convert_to_sql_plan(
        sql_engine_type=sql_client.sql_engine_type,
        dataflow_plan_node=plan.sink_node,
        optimization_level=SqlOptimizationLevel.O0,
        sql_query_plan_id=DagId.from_str("plan0"),
    )
    return sql_client.sql_plan_renderer.render_sql_plan(conversion_result.sql_plan).sql


def _inner_bookings_subquery_sql(rendered_sql: str) -> str:
    """SQL for the Metric('bookings') subquery, before the outer comparison against that metric."""
    inner_start = rendered_sql.find("LEFT OUTER JOIN (")
    outer_filter = rendered_sql.find("listing__bookings > 2")
    assert inner_start != -1 and outer_filter != -1 and inner_start < outer_filter
    return rendered_sql[inner_start:outer_filter]


def _assert_inner_bookings_subquery_has_constraints(rendered_sql: str) -> None:
    inner_sql = _inner_bookings_subquery_sql(rendered_sql)
    assert "Read Elements From Semantic Model 'bookings_source'" in inner_sql
    assert "listings_latest" not in inner_sql
    assert "WHERE listing = '1'" in inner_sql
    assert "metric_time__day BETWEEN '2020-01-01' AND '2020-01-02'" in inner_sql


def test_metric_filter_inherits_outer_time_and_entity_filters(
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """A metric-level Metric() subquery must apply the query time window and entity where.

    active_listings filters with Metric('bookings', group_by=['listing']). The bookings subquery has to
    carry the outer listing predicate and time window, not only the outer query.
    """
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("active_listings",),
        where_constraint_strs=["{{ Entity('listing') }} = '1'"],
        time_constraint_start=datetime.datetime(2020, 1, 1),
        time_constraint_end=datetime.datetime(2020, 1, 2),
    ).query_spec

    rendered_sql = _render_query_sql(
        dataflow_plan_builder=dataflow_plan_builder,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        query_spec=query_spec,
    )
    _assert_inner_bookings_subquery_has_constraints(rendered_sql)


def test_query_where_metric_filter_inherits_outer_time_and_entity_filters(
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Metric() in a query where also inherits the rest of the query filters and the time window."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        where_constraint_strs=[
            "{{ Metric('bookings', ['listing']) }} > 2",
            "{{ Entity('listing') }} = '1'",
        ],
        time_constraint_start=datetime.datetime(2020, 1, 1),
        time_constraint_end=datetime.datetime(2020, 1, 2),
    ).query_spec

    rendered_sql = _render_query_sql(
        dataflow_plan_builder=dataflow_plan_builder,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        query_spec=query_spec,
    )
    _assert_inner_bookings_subquery_has_constraints(rendered_sql)


def test_metric_filter_rejects_outer_filter_inner_metric_cannot_resolve(
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
) -> None:
    """An outer dimension the inner metric cannot resolve fails the compile instead of being dropped.

    bookings can filter on booking__is_instant. listings, used as Metric() in that where, cannot.
    """
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings",),
        where_constraint_strs=[
            "{{ Metric('listings', ['listing']) }} > 0",
            "{{ Dimension('booking__is_instant') }}",
        ],
    ).query_spec

    with pytest.raises(InvalidQueryException):
        dataflow_plan_builder.build_plan(query_spec)
