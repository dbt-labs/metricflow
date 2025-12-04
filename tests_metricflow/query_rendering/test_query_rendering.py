"""Tests base query rendering behavior by comparing rendered output against snapshot files.

This module is meant to start with a MetricFlowQuerySpec or equivalent representation of
a MetricFlow query input and end up with a query rendered for execution against the
target engine. This will depend on test semantic manifests and engine-specific rendering
logic as propagated via the SqlClient input.
"""

from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.test_utils import as_datetime
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.query_param_implementations import (
    DimensionOrEntityParameter,
    MetricParameter,
    OrderByParameter,
    SavedQueryParameter,
    TimeDimensionParameter,
)
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.metric_time_dimension import MTD_SPEC_DAY, MTD_SPEC_WEEK
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.query_rendering.compare_rendered_query import render_and_check


@pytest.mark.sql_engine_snapshot
def test_multihop_node(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    multihop_dataflow_plan_builder: DataflowPlanBuilder,
    multihop_dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a join between 1 simple-metric input and 2 dimensions."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="txn_count"),),
        dimension_specs=(
            DimensionSpec(
                element_name="customer_name",
                entity_links=(
                    EntityReference(element_name="account_id"),
                    EntityReference(element_name="customer_id"),
                ),
            ),
        ),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=multihop_dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=multihop_dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_filter_with_where_constraint_on_join_dim(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan to a SQL query plan where there is a join between 1 simple-metric input and 2 dimensions."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("bookings",),
        group_by_names=("booking__is_instant",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Dimension('listing__country_latest') }} = 'us'",
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
def test_partitioned_join(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests converting a dataflow plan where there's a join on a partitioned dimension."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="identity_verifications"),),
        dimension_specs=(
            DimensionSpec(
                element_name="home_state",
                entity_links=(EntityReference(element_name="user"),),
            ),
        ),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_limit_rows(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests a plan with a limit to the number of rows returned."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="bookings"),),
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="ds",
                entity_links=(),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
            ),
        ),
        limit=1,
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_distinct_values(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    column_association_resolver: ColumnAssociationResolver,
    sql_client: SqlClient,
) -> None:
    """Tests a plan to get distinct values for a dimension."""
    query_spec = query_parser.parse_and_validate_query(
        group_by_names=("listing__country_latest",),
        order_by_names=("-listing__country_latest",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Dimension('listing__country_latest') }} = 'us'",
            )
        ],
        limit=100,
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
def test_local_dimension_using_local_entity(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="listings"),),
        dimension_specs=(
            DimensionSpec(
                element_name="country_latest",
                entity_links=(EntityReference(element_name="listing"),),
            ),
        ),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_simple_metric_constraint(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("lux_booking_value_rate_expr",),
        group_by_names=(MTD_SPEC_DAY.dunder_name,),
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
def test_simple_metric_constraint_with_reused_simple_metric(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("instant_booking_value_ratio",),
        group_by_names=(MTD_SPEC_DAY.dunder_name,),
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
def test_simple_metric_constraint_with_single_expr_and_alias(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("double_counted_delayed_bookings",),
        group_by_names=(MTD_SPEC_DAY.dunder_name,),
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
def test_join_to_scd_dimension(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    scd_column_association_resolver: ColumnAssociationResolver,
    scd_query_parser: MetricFlowQueryParser,
    scd_dataflow_plan_builder: DataflowPlanBuilder,
    scd_dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests conversion of a plan using a dimension with a validity window inside a simple-metric input constraint."""
    query_spec = scd_query_parser.parse_and_validate_query(
        metric_names=("family_bookings",),
        group_by_names=(METRIC_TIME_ELEMENT_NAME,),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Dimension('listing__capacity') }} > 2",
            )
        ],
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=scd_dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=scd_dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_multi_hop_through_scd_dimension(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    scd_dataflow_plan_builder: DataflowPlanBuilder,
    scd_dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests conversion of a plan using a dimension that is reached through an SCD table."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="bookings"),),
        time_dimension_specs=(MTD_SPEC_DAY,),
        dimension_specs=(
            DimensionSpec(
                element_name="home_state_latest", entity_links=(EntityReference("listing"), EntityReference("user"))
            ),
        ),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=scd_dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=scd_dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_multi_hop_to_scd_dimension(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    scd_dataflow_plan_builder: DataflowPlanBuilder,
    scd_dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests conversion of a plan using an SCD dimension that is reached through another table."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="bookings"),),
        time_dimension_specs=(MTD_SPEC_DAY,),
        dimension_specs=(
            DimensionSpec(
                element_name="is_confirmed_lux",
                entity_links=(EntityReference("listing"), EntityReference("lux_listing")),
            ),
        ),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=scd_dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=scd_dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_multiple_metrics_no_dimensions(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="bookings"), MetricSpec(element_name="listings")),
        time_range_constraint=TimeRangeConstraint(
            start_time=as_datetime("2020-01-01"), end_time=as_datetime("2020-01-01")
        ),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_metric_with_simple_metrics_from_multiple_sources_no_dimensions(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="bookings_per_listing"),),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_common_semantic_model(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="bookings"), MetricSpec(element_name="booking_value")),
        dimension_specs=(MTD_SPEC_DAY,),
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_min_max_only_categorical(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests a min max only query with a categorical dimension."""
    query_spec = MetricFlowQuerySpec(
        dimension_specs=(
            DimensionSpec(
                element_name="country_latest",
                entity_links=(EntityReference(element_name="listing"),),
            ),
        ),
        min_max_only=True,
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_min_max_only_time(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests a min max only query with a time dimension."""
    query_spec = MetricFlowQuerySpec(
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="paid_at",
                entity_links=(EntityReference("booking"),),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
            ),
        ),
        min_max_only=True,
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_min_max_only_time_quarter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests a min max only query with a time dimension and non-default granularity."""
    query_spec = MetricFlowQuerySpec(
        time_dimension_specs=(
            TimeDimensionSpec(
                element_name="paid_at",
                entity_links=(EntityReference("booking"),),
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.QUARTER),
            ),
        ),
        min_max_only=True,
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_min_max_metric_time(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
) -> None:
    """Tests a plan to get the min & max distinct values of metric_time."""
    query_spec = MetricFlowQuerySpec(
        time_dimension_specs=(MTD_SPEC_DAY,),
        min_max_only=True,
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_min_max_metric_time_week(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
) -> None:
    """Tests a plan to get the min & max distinct values of metric_time with non-default granularity."""
    query_spec = MetricFlowQuerySpec(
        time_dimension_specs=(MTD_SPEC_WEEK,),
        min_max_only=True,
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_non_additive_dimension_with_non_default_grain(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
) -> None:
    """Tests querying a metric with a non-additive agg_time_dimension that has non-default granularity."""
    query_spec = MetricFlowQuerySpec(
        metric_specs=(MetricSpec(element_name="total_account_balance_first_day_of_month"),)
    )

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_semi_additive_measure_with_where_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    sql_client: SqlClient,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
) -> None:
    """Tests querying a semi-additive measure with a where filter."""
    query_spec = query_parser.parse_and_validate_query(
        metric_names=("current_account_balance_by_user",),
        group_by_names=("user",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Dimension('account__account_type') }} = 'savings'",
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
@pytest.mark.duckdb_only
def test_aliases_with_metrics(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
) -> None:
    """Tests a metric query with various aliases."""
    metric_param = MetricParameter(name="bookings", alias="bookings_alias")
    time_dimension_param = TimeDimensionParameter(name="metric_time__day", alias="booking_day")
    dimension_param = DimensionOrEntityParameter(name="listing__capacity_latest", alias="listing_capacity")
    entity_param = DimensionOrEntityParameter(name="listing", alias="listing_id")
    query_spec = query_parser.parse_and_validate_query(
        metrics=(metric_param,),
        group_by=(time_dimension_param, dimension_param, entity_param),
        order_by=(
            OrderByParameter(metric_param),
            OrderByParameter(time_dimension_param),
            OrderByParameter(dimension_param),
            OrderByParameter(entity_param),
        ),
        where_constraint_strs=("{{ Metric('booking_fees', ['listing']) }} > 2",),
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
@pytest.mark.duckdb_only
def test_aliases_without_metrics(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
) -> None:
    """Tests a plan with an aliased dimension."""
    dimension_param = DimensionOrEntityParameter(name="listing__capacity_latest", alias="listing_capacity")
    entity_param = DimensionOrEntityParameter(name="listing", alias="listing_id")
    query_spec = query_parser.parse_and_validate_query(
        group_by=(dimension_param, entity_param),
        order_by=(OrderByParameter(dimension_param), OrderByParameter(entity_param)),
        where_constraint_strs=("{{ Dimension('listing__capacity_latest') }} > 2",),
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
@pytest.mark.duckdb_only
def test_derived_metric_alias(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    sql_client: SqlClient,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
) -> None:
    """Tests a plan with an aliased metric."""
    metric = MetricParameter(name="booking_fees", alias="bookings_alias")

    query_spec = query_parser.parse_and_validate_query(
        metrics=(metric,),
        group_by_names=("metric_time__day",),
        order_by=(OrderByParameter(metric),),
        where_constraint_strs=("{{ Metric('booking_fees', ['listing']) }} > 2",),
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
def test_scd_dimension_filter_without_metric_time(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    scd_column_association_resolver: ColumnAssociationResolver,
    scd_query_parser: MetricFlowQueryParser,
    scd_dataflow_plan_builder: DataflowPlanBuilder,
    scd_dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = scd_query_parser.parse_and_validate_query(
        metric_names=("family_bookings",),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Dimension('listing__capacity') }} > 2",
            )
        ],
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=scd_dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=scd_dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_scd_dimension_group_by_without_metric_time(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    scd_column_association_resolver: ColumnAssociationResolver,
    scd_query_parser: MetricFlowQueryParser,
    scd_dataflow_plan_builder: DataflowPlanBuilder,
    scd_dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = scd_query_parser.parse_and_validate_query(
        metric_names=("family_bookings",),
        group_by_names=("listing__capacity",),
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=scd_dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=scd_dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_scd_group_by_and_coarser_grain(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    scd_query_parser: MetricFlowQueryParser,
    scd_dataflow_plan_builder: DataflowPlanBuilder,
    scd_dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = scd_query_parser.parse_and_validate_query(
        metric_names=("family_bookings",),
        group_by_names=("listing__capacity", "metric_time__month"),
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=scd_dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=scd_dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_no_dedupe(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    scd_query_parser: MetricFlowQueryParser,
    scd_dataflow_plan_builder: DataflowPlanBuilder,
    scd_dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = scd_query_parser.parse_and_validate_query(
        group_by_names=("listing__capacity", "metric_time__month"),
        apply_group_by=False,
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Dimension('user__home_state_latest') }} = 'CA'",
            )
        ],
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=scd_dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=scd_dataflow_plan_builder,
        query_spec=query_spec,
    )


@pytest.mark.sql_engine_snapshot
def test_no_dedupe_saved_query(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = query_parser.parse_and_validate_saved_query(
        saved_query_parameter=SavedQueryParameter(name="dimensions_only"),
        apply_group_by=False,
        where_filters=[
            PydanticWhereFilter(
                where_sql_template="{{ Dimension('user__home_state_latest') }} = 'CA'",
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
@pytest.mark.duckdb_only
def test_saved_query_with_order_by_and_limit(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = query_parser.parse_and_validate_saved_query(
        saved_query_parameter=SavedQueryParameter(name="p0_booking_with_order_by_and_limit")
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
@pytest.mark.duckdb_only
def test_saved_query_override_order_by_and_limit(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    query_spec = query_parser.parse_and_validate_saved_query(
        saved_query_parameter=SavedQueryParameter(name="p0_booking_with_order_by_and_limit"),
        order_by_names=["bookings", "views", "listing__capacity_latest", "metric_time__day"],
        limit=5,
    ).query_spec

    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=dataflow_plan_builder,
        query_spec=query_spec,
    )
