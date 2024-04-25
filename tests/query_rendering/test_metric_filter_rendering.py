from __future__ import annotations

import string

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from dbt_semantic_interfaces.references import MeasureReference

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.model.semantics.linkable_element_properties import LinkableElementProperty
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.query.query_parser import MetricFlowQueryParser
from tests.fixtures.setup_fixtures import MetricFlowTestConfiguration
from tests.query_rendering.compare_rendered_query import convert_and_check


@pytest.mark.sql_engine_snapshot
def test_query_with_simple_metric_in_where_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a simple metric in the query-level where filter."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        where_constraint=PydanticWhereFilter(
            where_sql_template="{{ Metric('bookings', ['listing']) }} > 2",
        ),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_metric_with_metric_in_where_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a metric in the metric-level where filter."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("active_listings",),
        group_by_names=("metric_time__day",),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_query_with_derived_metric_in_where_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a derived metric in the query-level where filter."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        where_constraint=PydanticWhereFilter(
            where_sql_template="{{ Metric('views_times_booking_value', ['listing']) }} > 1",
        ),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_query_with_ratio_metric_in_where_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a ratio metric in the query-level where filter."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        where_constraint=PydanticWhereFilter(
            where_sql_template="{{ Metric('bookings_per_booker', ['listing']) }} > 1",
        ),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_query_with_cumulative_metric_in_where_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a cumulative metric in the query-level where filter.

    Note this cumulative metric has no window / grain to date.
    """
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        where_constraint=PydanticWhereFilter(
            where_sql_template="{{ Metric('revenue_all_time', ['user']) }} > 1",
        ),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_query_with_multiple_metrics_in_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with 2 simple metrics in the query-level where filter."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        where_constraint=PydanticWhereFilter(
            where_sql_template="{{ Metric('bookings', ['listing']) }} > 2 AND {{ Metric('bookers', ['listing']) }} > 1",
        ),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_filter_by_metric_in_same_semantic_model_as_queried_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query with a simple metric in the query-level where filter."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookers",),
        where_constraint=PydanticWhereFilter(
            where_sql_template="{{ Metric('booking_value', ['guest']) }} > 1.00",
        ),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_distinct_values_query_with_metric_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a distinct values query with a metric in the query-level where filter."""
    query_spec = query_parser.parse_and_validate_query(
        group_by_names=("listing",),
        where_constraint=PydanticWhereFilter(
            where_sql_template="{{ Metric('bookings', ['listing']) }} > 2",
        ),
    )
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_metric_filtered_by_itself(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a query for a metric that filters by the same metric."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookers",),
        where_constraint=PydanticWhereFilter(
            where_sql_template="{{ Metric('bookers', ['guest']) }} > 1.00",
        ),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


@pytest.mark.sql_engine_snapshot
def test_multi_hop_with_explicit_entity_link(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a metric filter that requires multiple join hops and explicitly states the entity link."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("listings",),
        where_constraint=PydanticWhereFilter(
            where_sql_template="{{ Metric('instant_bookings', ['user__company']) }} > 2",
        ),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_output_nodes[0].parent_node,
    )


# TODO - need a different example because this one won't work due to ambiguous join path
# @pytest.mark.sql_engine_snapshot
# def test_multi_hop_without_explicit_entity_link(
#     request: FixtureRequest,
#     mf_test_configuration: MetricFlowTestConfiguration,
#     dataflow_plan_builder: DataflowPlanBuilder,
#     sql_client: SqlClient,
#     dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
#     query_parser: MetricFlowQueryParser,
# ) -> None:
#     """Tests a metric filter that requires multiple join hops and does not state the entity link.

#     Should return the same SQL as if the entity link was stated (group by resolution determines the entity link).
#     """
#     query_spec = query_parser.parse_and_validate_query(
#         metric_names=("listings",),
#         where_constraint=PydanticWhereFilter(
#             where_sql_template="{{ Metric('instant_bookings', ['company']) }} > 2",
#         ),
#     )
#     dataflow_plan = dataflow_plan_builder.build_plan(query_spec)

#     convert_and_check(
#         request=request,
#         mf_test_configuration=mf_test_configuration,
#         dataflow_to_sql_converter=dataflow_to_sql_converter,
#         sql_client=sql_client,
#         node=dataflow_plan.sink_output_nodes[0].parent_node,
#     )


# The group by shows you the path to get from the query-level measure to the entity, not the filter-level metric to the entity. That's confusing. Is that how it should be??


# TODO: move these to DFP tests
def test_all_available_single_hop_metric_filters(
    dataflow_plan_builder: DataflowPlanBuilder, query_parser: MetricFlowQueryParser
) -> None:
    """Checks that all allowed metric filters do not error when used in dataflow plan (single-hop)."""
    # We want group_by to take the group by entity with links for that one. Maybe we will add entity_links property later. Should be able to resolve automatically for most.
    # Priority: enable all single-hop metric joins. That means inner and outer entity links match.
    for group_by_metric in dataflow_plan_builder._metric_lookup.linkable_elements_for_measure(
        MeasureReference("listings"), without_any_of={LinkableElementProperty.MULTI_HOP}
    ).as_spec_set.group_by_metric_specs:
        print("looks like:", group_by_metric)
        try:
            entity_spec = group_by_metric.metric_subquery_entity_spec
            entity_name = "__".join(
                [entity_ref.element_name for entity_ref in entity_spec.entity_links] + [entity_spec.element_name]
            )
            query_spec = query_parser.parse_and_validate_query(
                metric_names=("listings",),
                where_constraint=PydanticWhereFilter(
                    where_sql_template=string.Template("{{ Metric('$metric_name', ['$entity_name']) }} > 2").substitute(
                        metric_name=group_by_metric.element_name, entity_name=entity_name
                    ),
                ),
            )
            dataflow_plan_builder.build_plan(query_spec)
            print("succeeded:", group_by_metric.element_name, entity_name)
        except Exception as e:
            print(f"failed with {e}:", group_by_metric.element_name, entity_name)
    assert 0


def test_all_available_multi_hop_metric_filters(
    dataflow_plan_builder: DataflowPlanBuilder, query_parser: MetricFlowQueryParser
) -> None:
    """Checks that all allowed metric filters do not error when used in dataflow plan (multi-hop)."""
    for group_by_metric in dataflow_plan_builder._metric_lookup.linkable_elements_for_measure(
        MeasureReference("listings"), with_any_of={LinkableElementProperty.MULTI_HOP}
    ).as_spec_set.group_by_metric_specs:
        try:
            entity_spec = group_by_metric.metric_subquery_entity_spec
            entity_name = "__".join(
                [entity_ref.element_name for entity_ref in entity_spec.entity_links] + [entity_spec.element_name]
            )
            query_spec = query_parser.parse_and_validate_query(
                metric_names=("listings",),
                where_constraint=PydanticWhereFilter(
                    where_sql_template=string.Template("{{ Metric('$metric_name', ['$entity_name']) }} > 2").substitute(
                        metric_name=group_by_metric.element_name, entity_name=entity_name
                    ),
                ),
            )
            dataflow_plan_builder.build_plan(query_spec)
            print("succeeded:", group_by_metric.element_name, entity_name)
        except Exception as e:
            print(f"failed with {e}:", group_by_metric.element_name, entity_name)
    # what's failing: appears to be one too many hops!
    assert 0
